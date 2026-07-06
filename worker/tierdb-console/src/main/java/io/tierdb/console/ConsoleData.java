package io.tierdb.console;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.StringJoiner;
import javax.sql.DataSource;

/** Catalog queries behind the console's JSON API. */
final class ConsoleData {

    private final DataSource dataSource;

    ConsoleData(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private static final String OVERVIEW = """
            SELECT t.table_id, t.schema_name, t.table_name, t.mode,
                   t.tier_key_col, t.lake_format, t.heap_retention_lag,
                   c.tier_key_hi, c.lake_snapshot_id, c.replicated_lsn,
                   extract(epoch FROM c.updated_at)::bigint,
                   (SELECT count(*) FROM tierdb.delta d WHERE d.table_id = t.table_id),
                   (SELECT count(*) FROM tierdb.read_pins p
                     WHERE p.table_id = t.table_id AND p.expires_at > now()),
                   cp.chunks_done,
                   extract(epoch FROM cp.updated_at)::bigint,
                   (SELECT nullif(reltuples, -1)::bigint FROM pg_class
                     WHERE oid = t.table_id::oid),
                   (SELECT jsonb_object_agg(s.state, s.n)
                      FROM (SELECT state, count(*) AS n FROM tierdb.partitions p
                             WHERE p.table_id = t.table_id GROUP BY state) s),
                   (SELECT count(*) FROM tierdb.load_labels l
                     WHERE l.table_id = t.table_id AND l.state = 'staged'),
                   ls.stats, jsonb_array_length(ls.warnings), t.tier_key_type
              FROM tierdb.tables t
              LEFT JOIN tierdb.cutline c USING (table_id)
              LEFT JOIN tierdb.copy_progress cp USING (table_id)
              LEFT JOIN tierdb.lake_stats ls USING (table_id)
             ORDER BY t.schema_name, t.table_name
            """;

    private static final String SLOTS = """
            SELECT slot_name, active,
                   COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)
              FROM pg_replication_slots WHERE slot_name LIKE 'tierdb\\_%'
            """;

    String overview(boolean leading) throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            long walLsn;
            try (ResultSet rs = s.executeQuery(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')")) {
                rs.next();
                walLsn = rs.getLong(1);
            }
            long schemaVersion;
            try (ResultSet rs = s.executeQuery("SELECT max(version) FROM tierdb.schema_meta")) {
                schemaVersion = rs.next() ? rs.getLong(1) : 0;
            }

            StringJoiner tables = new StringJoiner(",", "[", "]");
            try (ResultSet rs = s.executeQuery(OVERVIEW)) {
                while (rs.next()) {
                    Long frontier = longOrNull(rs, 10);
                    Long chunks = longOrNull(rs, 14);
                    tables.add("{"
                            + "\"id\":" + rs.getLong(1)
                            + ",\"schema\":" + Json.str(rs.getString(2))
                            + ",\"name\":" + Json.str(rs.getString(3))
                            + ",\"mode\":" + Json.str(rs.getString(4))
                            + ",\"tierKey\":" + Json.str(rs.getString(5))
                            + ",\"tierKeyType\":" + Json.str(rs.getString(21))
                            + ",\"lakeFormat\":" + Json.str(rs.getString(6))
                            + ",\"heapRetentionLag\":" + Json.num(longOrNull(rs, 7))
                            + ",\"cutlineT\":" + Json.num(longOrNull(rs, 8))
                            + ",\"cutlineS\":" + Json.num(longOrNull(rs, 9))
                            + ",\"frontier\":" + Json.num(frontier)
                            + ",\"lagBytes\":" + Json.num(
                                    frontier == null ? null : Math.max(0, walLsn - frontier))
                            + ",\"cutlineUpdatedAt\":" + Json.num(longOrNull(rs, 11))
                            + ",\"deltaBacklog\":" + rs.getLong(12)
                            + ",\"readPins\":" + rs.getLong(13)
                            + ",\"copying\":" + Json.bool(chunks != null)
                            + ",\"copyChunks\":" + Json.num(chunks)
                            + ",\"copyUpdatedAt\":" + Json.num(longOrNull(rs, 15))
                            + ",\"estRows\":" + Json.num(longOrNull(rs, 16))
                            + ",\"partitions\":" + Json.raw(rs.getString(17))
                            + ",\"stagedLoads\":" + rs.getLong(18)
                            + ",\"lake\":" + Json.raw(rs.getString(19))
                            + ",\"lakeWarnings\":" + Json.num(longOrNull(rs, 20))
                            + "}");
                }
            }

            StringJoiner slots = new StringJoiner(",", "[", "]");
            try (ResultSet rs = s.executeQuery(SLOTS)) {
                while (rs.next()) {
                    slots.add("{\"name\":" + Json.str(rs.getString(1))
                            + ",\"active\":" + Json.bool(rs.getBoolean(2))
                            + ",\"retainedBytes\":" + rs.getLong(3) + "}");
                }
            }

            return "{\"now\":" + System.currentTimeMillis() / 1000
                    + ",\"leader\":" + Json.bool(leading)
                    + ",\"schemaVersion\":" + schemaVersion
                    + ",\"walLsn\":" + walLsn
                    + ",\"tables\":" + tables
                    + ",\"slots\":" + slots + "}";
        }
    }

    private static final String TABLE = """
            SELECT t.schema_name, t.table_name, t.mode, t.primary_key_cols,
                   t.tier_key_col, t.lake_format, t.lake_table_ref, t.heap_retention_lag,
                   t.publication_name, t.slot_name,
                   extract(epoch FROM t.created_at)::bigint,
                   c.lake_props ->> 'metadata_location',
                   mr.requested_by,
                   extract(epoch FROM mr.requested_at)::bigint,
                   t.storage_profile, t.tier_key_type
              FROM tierdb.tables t
              LEFT JOIN tierdb.cutline c USING (table_id)
              LEFT JOIN tierdb.maintenance_requests mr USING (table_id)
             WHERE t.table_id = ?
            """;

    boolean requestMaintenance(long tableId) throws Exception {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO tierdb.maintenance_requests (table_id, requested_by)
                        SELECT table_id, 'console' FROM tierdb.tables WHERE table_id = ?
                        ON CONFLICT (table_id) DO UPDATE
                           SET requested_at = now(), requested_by = EXCLUDED.requested_by
                        """)) {
            ps.setLong(1, tableId);
            return ps.executeUpdate() > 0;
        }
    }

    private static final String LAKE = """
            SELECT stats, warnings, policy, extract(epoch FROM collected_at)::bigint
              FROM tierdb.lake_stats WHERE table_id = ?
            """;

    private static final String PARTITIONS = """
            SELECT partition_id, tier_key_lo, tier_key_hi, state,
                   extract(epoch FROM updated_at)::bigint
              FROM tierdb.partitions WHERE table_id = ?
             ORDER BY tier_key_lo DESC LIMIT 200
            """;

    private static final String OPS = """
            SELECT op_id, op_kind, phase, lake_snapshot_id, details,
                   extract(epoch FROM updated_at)::bigint
              FROM tierdb.op_log WHERE table_id = ?
             ORDER BY updated_at DESC LIMIT 50
            """;

    String table(long id) throws Exception {
        try (Connection c = dataSource.getConnection()) {
            StringBuilder out = new StringBuilder("{");
            try (PreparedStatement ps = c.prepareStatement(TABLE)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    StringJoiner pk = new StringJoiner(",", "[", "]");
                    for (Object col : (Object[]) rs.getArray(4).getArray()) {
                        pk.add(Json.str((String) col));
                    }
                    out.append("\"id\":").append(id)
                            .append(",\"schema\":").append(Json.str(rs.getString(1)))
                            .append(",\"name\":").append(Json.str(rs.getString(2)))
                            .append(",\"mode\":").append(Json.str(rs.getString(3)))
                            .append(",\"pk\":").append(pk)
                            .append(",\"tierKey\":").append(Json.str(rs.getString(5)))
                            .append(",\"tierKeyType\":").append(Json.str(rs.getString(16)))
                            .append(",\"lakeFormat\":").append(Json.str(rs.getString(6)))
                            .append(",\"lakeRef\":").append(Json.str(rs.getString(7)))
                            .append(",\"heapRetentionLag\":").append(Json.num(longOrNull(rs, 8)))
                            .append(",\"publication\":").append(Json.str(rs.getString(9)))
                            .append(",\"slot\":").append(Json.str(rs.getString(10)))
                            .append(",\"createdAt\":").append(Json.num(longOrNull(rs, 11)))
                            .append(",\"metadataLocation\":").append(Json.str(rs.getString(12)))
                            .append(",\"storageProfile\":").append(Json.str(rs.getString(15)));
                    String requestedBy = rs.getString(13);
                    out.append(",\"maintenancePending\":");
                    if (requestedBy == null) {
                        out.append("null");
                    } else {
                        out.append("{\"requestedBy\":").append(Json.str(requestedBy))
                                .append(",\"requestedAt\":").append(Json.num(longOrNull(rs, 14)))
                                .append('}');
                    }
                }
            }

            out.append(",\"lake\":");
            try (PreparedStatement ps = c.prepareStatement(LAKE)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        out.append("{\"stats\":").append(Json.raw(rs.getString(1)))
                                .append(",\"warnings\":").append(Json.raw(rs.getString(2)))
                                .append(",\"policy\":").append(Json.raw(rs.getString(3)))
                                .append(",\"collectedAt\":").append(Json.num(longOrNull(rs, 4)))
                                .append('}');
                    } else {
                        out.append("null");
                    }
                }
            }

            StringJoiner partitions = new StringJoiner(",", "[", "]");
            try (PreparedStatement ps = c.prepareStatement(PARTITIONS)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        partitions.add("{\"id\":" + Json.str(rs.getString(1))
                                + ",\"lo\":" + rs.getLong(2)
                                + ",\"hi\":" + rs.getLong(3)
                                + ",\"state\":" + Json.str(rs.getString(4))
                                + ",\"updatedAt\":" + Json.num(longOrNull(rs, 5)) + "}");
                    }
                }
            }
            out.append(",\"partitions\":").append(partitions);

            StringJoiner ops = new StringJoiner(",", "[", "]");
            try (PreparedStatement ps = c.prepareStatement(OPS)) {
                ps.setLong(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ops.add("{\"opId\":" + Json.str(rs.getString(1))
                                + ",\"kind\":" + Json.str(rs.getString(2))
                                + ",\"phase\":" + Json.str(rs.getString(3))
                                + ",\"snapshot\":" + Json.num(longOrNull(rs, 4))
                                + ",\"details\":" + Json.raw(rs.getString(5))
                                + ",\"updatedAt\":" + Json.num(longOrNull(rs, 6)) + "}");
                    }
                }
            }
            out.append(",\"ops\":").append(ops);

            return out.append('}').toString();
        }
    }

    private static final String PROFILES = """
            SELECT p.profile_name, p.lake_format, p.warehouse, p.lake_config::text,
                   p.credential_ref, p.is_default,
                   extract(epoch FROM p.created_at)::bigint,
                   (SELECT count(*) FROM tierdb.tables t
                     WHERE t.storage_profile = p.profile_name)
              FROM tierdb.storage_profiles p
             ORDER BY p.is_default DESC, p.profile_name
            """;

    String storageProfiles() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(PROFILES)) {
            StringJoiner out = new StringJoiner(",", "[", "]");
            while (rs.next()) {
                out.add("{\"name\":" + Json.str(rs.getString(1))
                        + ",\"lakeFormat\":" + Json.str(rs.getString(2))
                        + ",\"warehouse\":" + Json.str(rs.getString(3))
                        + ",\"lakeConfig\":" + Json.raw(rs.getString(4))
                        + ",\"credentialRef\":" + Json.str(rs.getString(5))
                        + ",\"isDefault\":" + Json.bool(rs.getBoolean(6))
                        + ",\"createdAt\":" + Json.num(longOrNull(rs, 7))
                        + ",\"tables\":" + rs.getLong(8) + "}");
            }
            return "{\"profiles\":" + out + "}";
        }
    }

    void createStorageProfile(String name, String lakeFormat, String warehouse,
            String lakeConfigJson, String credentialRef, boolean isDefault) throws Exception {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("""
                        INSERT INTO tierdb.storage_profiles
                            (profile_name, lake_format, warehouse, lake_config,
                             credential_ref, is_default)
                        VALUES (?, ?, ?, ?::jsonb, ?, ?)
                        """)) {
            ps.setString(1, name);
            ps.setString(2, lakeFormat);
            ps.setString(3, warehouse);
            ps.setString(4, lakeConfigJson);
            ps.setString(5, credentialRef);
            ps.setBoolean(6, isDefault);
            ps.executeUpdate();
        }
    }

    private static Long longOrNull(ResultSet rs, int col) throws Exception {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }
}
