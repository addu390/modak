package io.modak.catalog;

import io.modak.common.Cutline;
import io.modak.common.DeltaBatch;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * Authoritative {@link Catalog} over the {@code modak.*} tables in Postgres.
 * Consistency is enforced in SQL: {@code advanceCutline} is a single guarded,
 * monotonic {@code UPDATE}, and {@code transition} guards the {@code from} state
 * in its {@code WHERE} clause so a stale transition updates zero rows.
 */
public final class JdbcCatalog implements Catalog {

    private final Db db;

    public JdbcCatalog(DataSource dataSource) {
        this.db = new Db(Objects.requireNonNull(dataSource));
    }

    private static final String INSERT_TABLE = """
            INSERT INTO modak.tables
                (table_id, schema_name, table_name, primary_key_cols, tier_key_col,
                 partition_scheme, lake_format, lake_table_ref, lake_props,
                 mode, publication_name, slot_name, heap_retention_lag, lake_retention_lag)
            VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)
            """;

    @Override
    public TableId register(TableRegistration r) {
        db.update(INSERT_TABLE, ps -> {
            Array pk = ps.getConnection().createArrayOf("text", r.primaryKeyCols().toArray());
            ps.setLong(1, r.oid());
            ps.setString(2, r.schemaName());
            ps.setString(3, r.tableName());
            ps.setArray(4, pk);
            ps.setString(5, r.tierKeyCol());
            ps.setString(6, r.partitionScheme());
            ps.setString(7, r.lakeFormat());
            ps.setString(8, r.lakeTableRef());
            setNullableString(ps, 9, r.lakeProps());
            ps.setString(10, r.mode().sql());
            setNullableString(ps, 11, r.publicationName());
            setNullableString(ps, 12, r.slotName());
            setNullableLong(ps, 13, r.heapRetentionLag());
            setNullableLong(ps, 14, r.lakeRetentionLag());
        });
        return new TableId(r.oid());
    }

    @Override
    public boolean unregister(TableId table) {
        return db.update(
                "DELETE FROM modak.tables WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid())) > 0;
    }

    @Override
    public Optional<RegisteredTable> lookup(String schemaName, String tableName) {
        return db.queryOne(
                "SELECT * FROM modak.tables WHERE schema_name = ? AND table_name = ?",
                ps -> {
                    ps.setString(1, schemaName);
                    ps.setString(2, tableName);
                },
                JdbcCatalog::mapTable);
    }

    @Override
    public Optional<RegisteredTable> get(TableId table) {
        return db.queryOne(
                "SELECT * FROM modak.tables WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                JdbcCatalog::mapTable);
    }

    @Override
    public List<RegisteredTable> listTables() {
        return db.queryList(
                "SELECT * FROM modak.tables ORDER BY schema_name, table_name",
                Db.NO_ARGS,
                JdbcCatalog::mapTable);
    }

    @Override
    public void initCutline(TableId table, TierKey t, LakeSnapshotId snapshot) {
        db.update(
                "INSERT INTO modak.cutline (table_id, tier_key_hi, lake_snapshot_id) VALUES (?, ?, ?)",
                ps -> {
                    ps.setLong(1, table.oid());
                    ps.setLong(2, t.value());
                    ps.setLong(3, snapshot.id());
                });
    }

    @Override
    public Cutline readCutline(TableId table) {
        return db.queryOne(
                "SELECT tier_key_hi, lake_snapshot_id FROM modak.cutline WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> new Cutline(new TierKey(rs.getLong(1)), new LakeSnapshotId(rs.getLong(2))))
                .orElseThrow(() -> new CatalogException("no cut-line for table " + table));
    }

    private static final String ADVANCE_CUTLINE = """
            UPDATE modak.cutline
               SET tier_key_hi = ?, lake_snapshot_id = ?, updated_at = now()
             WHERE table_id = ? AND tier_key_hi <= ? AND lake_snapshot_id <= ?
            """;

    @Override
    public void advanceCutline(TableId table, TierKey newT, LakeSnapshotId snapshot) {
        int updated = db.update(ADVANCE_CUTLINE, ps -> {
            ps.setLong(1, newT.value());
            ps.setLong(2, snapshot.id());
            ps.setLong(3, table.oid());
            ps.setLong(4, newT.value());
            ps.setLong(5, snapshot.id());
        });
        if (updated == 0) {
            throw new CatalogException("advanceCutline rejected (missing or non-monotonic) for "
                    + table + " -> (" + newT + ", " + snapshot + ")");
        }
    }

    private static final String PATCH_LAKE_PROPS = """
            UPDATE modak.tables
               SET lake_props = coalesce(lake_props, '{}'::jsonb) || ?::jsonb
             WHERE table_id = ?
            """;

    @Override
    public void advanceCutline(TableId table, TierKey newT, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch) {
        // Cut-line and lake_props must move in one transaction, never independently.
        db.inTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement(ADVANCE_CUTLINE)) {
                ps.setLong(1, newT.value());
                ps.setLong(2, snapshot.id());
                ps.setLong(3, table.oid());
                ps.setLong(4, newT.value());
                ps.setLong(5, snapshot.id());
                if (ps.executeUpdate() == 0) {
                    throw new CatalogException("advanceCutline rejected (missing or non-monotonic) for "
                            + table + " -> (" + newT + ", " + snapshot + ")");
                }
            }
            if (!lakePropsPatch.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(PATCH_LAKE_PROPS)) {
                    ps.setString(1, jsonObject(lakePropsPatch));
                    ps.setLong(2, table.oid());
                    ps.executeUpdate();
                }
            }
            return null;
        });
    }

    private static final String ADVANCE_RETENTION = """
            UPDATE modak.cutline
               SET tier_key_hi = ?, updated_at = now()
             WHERE table_id = ? AND tier_key_hi <= ?
            """;

    @Override
    public void advanceRetentionLine(TableId table, TierKey newT) {
        int updated = db.update(ADVANCE_RETENTION, ps -> {
            ps.setLong(1, newT.value());
            ps.setLong(2, table.oid());
            ps.setLong(3, newT.value());
        });
        if (updated == 0) {
            throw new CatalogException("advanceRetentionLine rejected (missing or non-monotonic) for "
                    + table + " -> " + newT);
        }
    }

    @Override
    public Optional<Lsn> readMirrorFrontier(TableId table) {
        return db.queryOne(
                "SELECT replicated_lsn FROM modak.cutline WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> {
                    long lsn = rs.getLong(1);
                    return rs.wasNull() ? null : new Lsn(lsn);
                });
    }

    // The first advance seeds a null frontier; after that it is strictly monotonic.
    private static final String ADVANCE_FRONTIER = """
            UPDATE modak.cutline
               SET replicated_lsn = ?, lake_snapshot_id = ?, updated_at = now()
             WHERE table_id = ?
               AND coalesce(replicated_lsn, -9223372036854775808) <= ?
               AND lake_snapshot_id <= ?
            """;

    @Override
    public void advanceMirrorFrontier(TableId table, Lsn lsn, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch) {
        // Frontier and lake_props must move in one transaction, never independently.
        db.inTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement(ADVANCE_FRONTIER)) {
                ps.setLong(1, lsn.value());
                ps.setLong(2, snapshot.id());
                ps.setLong(3, table.oid());
                ps.setLong(4, lsn.value());
                ps.setLong(5, snapshot.id());
                if (ps.executeUpdate() == 0) {
                    throw new CatalogException("advanceMirrorFrontier rejected (missing or "
                            + "non-monotonic) for " + table + " -> (" + lsn + ", " + snapshot + ")");
                }
            }
            if (!lakePropsPatch.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(PATCH_LAKE_PROPS)) {
                    ps.setString(1, jsonObject(lakePropsPatch));
                    ps.setLong(2, table.oid());
                    ps.executeUpdate();
                }
            }
            return null;
        });
    }

    private static final String ADVANCE_SNAPSHOT = """
            UPDATE modak.cutline
               SET lake_snapshot_id = ?, updated_at = now()
             WHERE table_id = ? AND lake_snapshot_id <= ?
            """;

    private static final String CLEAR_DELTA_ROW = """
            DELETE FROM modak.delta
             WHERE table_id = ? AND pk = ? AND version <= ?
            """;

    private static final String COUNT_PINS = """
            SELECT count(*) FROM modak.read_pins
             WHERE table_id = ? AND expires_at > now()
            """;

    @Override
    public void publishCompaction(TableId table, LakeSnapshotId snapshot, DeltaBatch folded,
            Map<String, String> lakePropsPatch) {
        db.inTransaction(c -> {
            // A reader pinned since the pre-check still merges these delta rows.
            try (PreparedStatement ps = c.prepareStatement(COUNT_PINS)) {
                ps.setLong(1, table.oid());
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    if (rs.getLong(1) > 0) {
                        throw new CatalogException(
                                "publishCompaction blocked by active read pins for " + table);
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement(ADVANCE_SNAPSHOT)) {
                ps.setLong(1, snapshot.id());
                ps.setLong(2, table.oid());
                ps.setLong(3, snapshot.id());
                if (ps.executeUpdate() == 0) {
                    throw new CatalogException("publishCompaction snapshot regressed for " + table);
                }
            }
            try (PreparedStatement ps = c.prepareStatement(CLEAR_DELTA_ROW)) {
                for (DeltaBatch.Key key : folded.keys()) {
                    ps.setLong(1, table.oid());
                    ps.setString(2, key.pk());
                    ps.setLong(3, key.version());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            if (!lakePropsPatch.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(PATCH_LAKE_PROPS)) {
                    ps.setString(1, jsonObject(lakePropsPatch));
                    ps.setLong(2, table.oid());
                    ps.executeUpdate();
                }
            }
            return null;
        });
    }

    private static final String ADVANCE_RETENTION_LINE = """
            UPDATE modak.cutline
               SET retention_line = ?, updated_at = now()
             WHERE table_id = ? AND coalesce(retention_line, -9223372036854775808) <= ?
            """;

    private static final String PURGE_DELTA_BELOW = """
            DELETE FROM modak.delta WHERE table_id = ? AND tier_key < ?
            """;

    private static final String DROP_EXPIRED_PARTITIONS = """
            DELETE FROM modak.partitions
             WHERE table_id = ? AND tier_key_hi <= ? AND state = 'dropped'
            """;

    @Override
    public void publishRetention(TableId table, LakeSnapshotId snapshot, TierKey below,
            Map<String, String> lakePropsPatch) {
        db.inTransaction(c -> {
            // A reader pinned since the pre-check still merges purged delta rows.
            try (PreparedStatement ps = c.prepareStatement(COUNT_PINS)) {
                ps.setLong(1, table.oid());
                try (var rs = ps.executeQuery()) {
                    rs.next();
                    if (rs.getLong(1) > 0) {
                        throw new CatalogException(
                                "publishRetention blocked by active read pins for " + table);
                    }
                }
            }
            if (snapshot != null) {
                try (PreparedStatement ps = c.prepareStatement(ADVANCE_SNAPSHOT)) {
                    ps.setLong(1, snapshot.id());
                    ps.setLong(2, table.oid());
                    ps.setLong(3, snapshot.id());
                    if (ps.executeUpdate() == 0) {
                        throw new CatalogException("publishRetention snapshot regressed for " + table);
                    }
                }
            }
            try (PreparedStatement ps = c.prepareStatement(ADVANCE_RETENTION_LINE)) {
                ps.setLong(1, below.value());
                ps.setLong(2, table.oid());
                ps.setLong(3, below.value());
                if (ps.executeUpdate() == 0) {
                    throw new CatalogException("publishRetention rejected (missing or "
                            + "non-monotonic) for " + table + " -> " + below);
                }
            }
            try (PreparedStatement ps = c.prepareStatement(PURGE_DELTA_BELOW)) {
                ps.setLong(1, table.oid());
                ps.setLong(2, below.value());
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement(DROP_EXPIRED_PARTITIONS)) {
                ps.setLong(1, table.oid());
                ps.setLong(2, below.value());
                ps.executeUpdate();
            }
            if (!lakePropsPatch.isEmpty()) {
                try (PreparedStatement ps = c.prepareStatement(PATCH_LAKE_PROPS)) {
                    ps.setString(1, jsonObject(lakePropsPatch));
                    ps.setLong(2, table.oid());
                    ps.executeUpdate();
                }
            }
            return null;
        });
    }

    @Override
    public Optional<TierKey> readRetentionLine(TableId table) {
        return db.queryOne(
                "SELECT retention_line FROM modak.cutline WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> {
                    long line = rs.getLong(1);
                    return rs.wasNull() ? null : new TierKey(line);
                });
    }

    // Independent minima across pins is conservative and always safe.
    private static final String READ_HORIZON = """
            SELECT min(pinned_tier_key_hi), min(pinned_lake_snapshot_id)
              FROM modak.read_pins WHERE table_id = ? AND expires_at > now()
            """;

    @Override
    public Cutline readHorizon(TableId table) {
        return pinnedHorizon(table).orElseGet(() -> readCutline(table));
    }

    @Override
    public Optional<Cutline> pinnedHorizon(TableId table) {
        return db.queryOne(READ_HORIZON,
                ps -> ps.setLong(1, table.oid()),
                rs -> {
                    long t = rs.getLong(1);
                    if (rs.wasNull()) {
                        return null;
                    }
                    return new Cutline(new TierKey(t), new LakeSnapshotId(rs.getLong(2)));
                });
    }

    private static final String UPSERT_OP = """
            INSERT INTO modak.tiering_log (op_id, table_id, op_kind, phase, lake_snapshot_id, details)
            VALUES (?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (op_id) DO UPDATE
               SET phase = EXCLUDED.phase,
                   lake_snapshot_id = coalesce(EXCLUDED.lake_snapshot_id, modak.tiering_log.lake_snapshot_id),
                   details = coalesce(EXCLUDED.details, modak.tiering_log.details),
                   updated_at = now()
            """;

    @Override
    public void logOpPhase(UUID opId, TableId table, String opKind, String phase,
            LakeSnapshotId snapshot, String detailsJson) {
        db.update(UPSERT_OP, ps -> {
            ps.setObject(1, opId);
            ps.setLong(2, table.oid());
            ps.setString(3, opKind);
            ps.setString(4, phase);
            if (snapshot == null) {
                ps.setNull(5, Types.BIGINT);
            } else {
                ps.setLong(5, snapshot.id());
            }
            setNullableString(ps, 6, detailsJson);
        });
    }

    @Override
    public List<TieringOp> findIncompleteOps(TableId table, String opKind) {
        return db.queryList(
                "SELECT op_id, phase, lake_snapshot_id, details FROM modak.tiering_log "
                        + "WHERE table_id = ? AND op_kind = ? AND phase NOT IN (?, ?) "
                        + "ORDER BY updated_at",
                ps -> {
                    ps.setLong(1, table.oid());
                    ps.setString(2, opKind);
                    ps.setString(3, TieringOp.PHASE_ADVANCED);
                    ps.setString(4, TieringOp.PHASE_ABANDONED);
                },
                rs -> {
                    long snap = rs.getLong("lake_snapshot_id");
                    boolean noSnap = rs.wasNull();
                    return new TieringOp(
                            rs.getObject("op_id", UUID.class),
                            table,
                            opKind,
                            rs.getString("phase"),
                            noSnap ? Optional.empty() : Optional.of(new LakeSnapshotId(snap)),
                            rs.getString("details"));
                });
    }

    private static final String UPSERT_PARTITION = """
            INSERT INTO modak.partitions
                (table_id, partition_id, tier_key_lo, tier_key_hi, state)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (table_id, partition_id) DO UPDATE
               SET tier_key_lo = EXCLUDED.tier_key_lo,
                   tier_key_hi = EXCLUDED.tier_key_hi,
                   state = EXCLUDED.state,
                   updated_at = now()
            """;

    @Override
    public void upsertPartition(PartitionId id, PartitionBounds bounds, PartitionState state) {
        db.update(UPSERT_PARTITION, ps -> {
            ps.setLong(1, id.table().oid());
            ps.setString(2, id.id());
            ps.setLong(3, bounds.lo().value());
            ps.setLong(4, bounds.hi().value());
            ps.setString(5, state.name().toLowerCase());
        });
    }

    @Override
    public List<PartitionInfo> listPartitions(TableId table) {
        return db.queryList(
                "SELECT partition_id, tier_key_lo, tier_key_hi, state "
                        + "FROM modak.partitions WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> new PartitionInfo(
                        new PartitionId(table, rs.getString("partition_id")),
                        new PartitionBounds(
                                new TierKey(rs.getLong("tier_key_lo")),
                                new TierKey(rs.getLong("tier_key_hi"))),
                        PartitionState.valueOf(rs.getString("state").toUpperCase())));
    }

    private static final String TRANSITION = """
            UPDATE modak.partitions
               SET state = ?, updated_at = now()
             WHERE table_id = ? AND partition_id = ? AND state = ?
            """;

    @Override
    public void transition(PartitionId partition, PartitionState from, PartitionState to) {
        if (!from.canTransitionTo(to)) {
            throw new IllegalTransitionException(partition, from, to);
        }
        int updated = db.update(TRANSITION, ps -> {
            ps.setString(1, to.name().toLowerCase());
            ps.setLong(2, partition.table().oid());
            ps.setString(3, partition.id());
            ps.setString(4, from.name().toLowerCase());
        });
        if (updated == 0) {
            throw new CatalogException("transition rejected (missing or not in state " + from
                    + ") for " + partition);
        }
    }

    private static RegisteredTable mapTable(ResultSet rs) throws SQLException {
        Array pkArray = rs.getArray("primary_key_cols");
        List<String> pkCols = List.of((String[]) pkArray.getArray());
        return new RegisteredTable(
                new TableId(rs.getLong("table_id")),
                rs.getString("schema_name"),
                rs.getString("table_name"),
                pkCols,
                rs.getString("tier_key_col"),
                rs.getString("partition_scheme"),
                rs.getString("lake_format"),
                rs.getString("lake_table_ref"),
                rs.getString("lake_props"),
                rs.getInt("schema_version"),
                TableMode.fromSql(rs.getString("mode")),
                rs.getString("publication_name"),
                rs.getString("slot_name"),
                nullableLong(rs, "heap_retention_lag"),
                nullableLong(rs, "lake_retention_lag"));
    }

    private static Optional<Long> nullableLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? Optional.empty() : Optional.of(value);
    }

    private static void setNullableLong(PreparedStatement ps, int idx, Optional<Long> value)
            throws SQLException {
        if (value.isPresent()) {
            ps.setLong(idx, value.get());
        } else {
            ps.setNull(idx, Types.BIGINT);
        }
    }

    private static void setNullableString(PreparedStatement ps, int idx, String value) throws SQLException {
        if (value == null) {
            ps.setNull(idx, Types.VARCHAR);
        } else {
            ps.setString(idx, value);
        }
    }

    static String jsonObject(Map<String, String> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            jsonString(sb, e.getKey());
            sb.append(':');
            jsonString(sb, e.getValue());
        }
        return sb.append('}').toString();
    }

    private static void jsonString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
        sb.append('"');
    }
}
