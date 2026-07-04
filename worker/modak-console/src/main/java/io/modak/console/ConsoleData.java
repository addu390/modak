package io.modak.console;

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
                   (SELECT count(*) FROM modak.delta d WHERE d.table_id = t.table_id),
                   (SELECT count(*) FROM modak.read_pins p
                     WHERE p.table_id = t.table_id AND p.expires_at > now()),
                   cp.chunks_done,
                   extract(epoch FROM cp.updated_at)::bigint,
                   (SELECT nullif(reltuples, -1)::bigint FROM pg_class
                     WHERE oid = t.table_id::oid),
                   (SELECT jsonb_object_agg(s.state, s.n)
                      FROM (SELECT state, count(*) AS n FROM modak.partitions p
                             WHERE p.table_id = t.table_id GROUP BY state) s),
                   (SELECT count(*) FROM modak.load_labels l
                     WHERE l.table_id = t.table_id AND l.state = 'staged')
              FROM modak.tables t
              LEFT JOIN modak.cutline c USING (table_id)
              LEFT JOIN modak.copy_progress cp USING (table_id)
             ORDER BY t.schema_name, t.table_name
            """;

    private static final String SLOTS = """
            SELECT slot_name, active,
                   COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)
              FROM pg_replication_slots WHERE slot_name LIKE 'modak\\_%'
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
            try (ResultSet rs = s.executeQuery("SELECT max(version) FROM modak.schema_meta")) {
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
                   t.lake_props ->> 'metadata_location'
              FROM modak.tables t WHERE t.table_id = ?
            """;

    private static final String PARTITIONS = """
            SELECT partition_id, tier_key_lo, tier_key_hi, state,
                   extract(epoch FROM updated_at)::bigint
              FROM modak.partitions WHERE table_id = ?
             ORDER BY tier_key_lo DESC LIMIT 200
            """;

    private static final String OPS = """
            SELECT op_id, op_kind, phase, lake_snapshot_id, details,
                   extract(epoch FROM updated_at)::bigint
              FROM modak.tiering_log WHERE table_id = ?
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
                            .append(",\"lakeFormat\":").append(Json.str(rs.getString(6)))
                            .append(",\"lakeRef\":").append(Json.str(rs.getString(7)))
                            .append(",\"heapRetentionLag\":").append(Json.num(longOrNull(rs, 8)))
                            .append(",\"publication\":").append(Json.str(rs.getString(9)))
                            .append(",\"slot\":").append(Json.str(rs.getString(10)))
                            .append(",\"createdAt\":").append(Json.num(longOrNull(rs, 11)))
                            .append(",\"metadataLocation\":").append(Json.str(rs.getString(12)));
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

    private static Long longOrNull(ResultSet rs, int col) throws Exception {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }
}
