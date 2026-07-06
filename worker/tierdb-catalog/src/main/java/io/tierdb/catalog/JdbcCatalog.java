package io.tierdb.catalog;

import io.tierdb.common.Cutline;
import io.tierdb.common.DeltaBatch;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.Lsn;
import io.tierdb.common.OpKind;
import io.tierdb.common.OpPhase;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.common.TierKeyType;
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

/** Authoritative {@link Catalog} over the {@code tierdb.*} tables in Postgres. */
public final class JdbcCatalog implements Catalog {

    private final Db db;

    public JdbcCatalog(DataSource dataSource) {
        this.db = new Db(Objects.requireNonNull(dataSource));
    }

    private static final String INSERT_TABLE = """
            INSERT INTO tierdb.tables
                (table_id, schema_name, table_name, primary_key_cols, tier_key_col,
                 tier_key_type, partition_scheme, lake_format, lake_table_ref, storage_profile,
                 mode, publication_name, slot_name, heap_retention_lag, lake_retention_lag,
                 keep_heap)
            VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ps.setString(6, r.tierKeyType().sql());
            ps.setString(7, r.partitionScheme());
            ps.setString(8, r.lakeFormat());
            ps.setString(9, r.lakeTableRef());
            ps.setString(10, r.storageProfile());
            ps.setString(11, r.mode().sql());
            setNullableString(ps, 12, r.publicationName());
            setNullableString(ps, 13, r.slotName());
            setNullableLong(ps, 14, r.heapRetentionLag());
            setNullableLong(ps, 15, r.lakeRetentionLag());
            ps.setBoolean(16, r.keepHeap());
        });
        return new TableId(r.oid());
    }

    private static final String PROFILE_COLS =
            "profile_name, lake_format, warehouse, lake_config::text, credential_ref, is_default";

    @Override
    public List<StorageProfile> listStorageProfiles() {
        return db.queryList(
                "SELECT " + PROFILE_COLS + " FROM tierdb.storage_profiles "
                        + "ORDER BY is_default DESC, profile_name",
                Db.NO_ARGS,
                JdbcCatalog::mapProfile);
    }

    @Override
    public Optional<StorageProfile> storageProfile(String name) {
        return db.queryOne(
                "SELECT " + PROFILE_COLS + " FROM tierdb.storage_profiles WHERE profile_name = ?",
                ps -> ps.setString(1, name),
                JdbcCatalog::mapProfile);
    }

    @Override
    public StorageProfile defaultStorageProfile() {
        return db.queryOne(
                "SELECT " + PROFILE_COLS + " FROM tierdb.storage_profiles WHERE is_default",
                Db.NO_ARGS,
                JdbcCatalog::mapProfile)
                .orElseThrow(() -> new CatalogException(
                        "no default storage profile (catalog.sql seeds one, was it applied?)"));
    }

    @Override
    public void createStorageProfile(StorageProfile p) {
        db.update("""
                INSERT INTO tierdb.storage_profiles
                    (profile_name, lake_format, warehouse, lake_config, credential_ref, is_default)
                VALUES (?, ?, ?, ?::jsonb, ?, ?)
                """, ps -> {
            ps.setString(1, p.name());
            setNullableString(ps, 2, p.lakeFormat());
            ps.setString(3, p.warehouse());
            setNullableString(ps, 4, p.lakeConfigJson());
            setNullableString(ps, 5, p.credentialRef());
            ps.setBoolean(6, p.isDefault());
        });
    }

    private static StorageProfile mapProfile(ResultSet rs) throws SQLException {
        return new StorageProfile(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3),
                rs.getString(4),
                rs.getString(5),
                rs.getBoolean(6));
    }

    @Override
    public boolean unregister(TableId table) {
        return db.update(
                "DELETE FROM tierdb.tables WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid())) > 0;
    }

    @Override
    public Optional<RegisteredTable> lookup(String schemaName, String tableName) {
        return db.queryOne(
                "SELECT * FROM tierdb.tables WHERE schema_name = ? AND table_name = ?",
                ps -> {
                    ps.setString(1, schemaName);
                    ps.setString(2, tableName);
                },
                JdbcCatalog::mapTable);
    }

    @Override
    public Optional<RegisteredTable> get(TableId table) {
        return db.queryOne(
                "SELECT * FROM tierdb.tables WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                JdbcCatalog::mapTable);
    }

    @Override
    public List<RegisteredTable> listTables() {
        return db.queryList(
                "SELECT * FROM tierdb.tables ORDER BY schema_name, table_name",
                Db.NO_ARGS,
                JdbcCatalog::mapTable);
    }

    @Override
    public void setMaintenancePolicy(TableId table, MaintenancePolicy policy) {
        int updated = db.update(
                "UPDATE tierdb.tables SET maintenance_policy = ?::jsonb WHERE table_id = ?",
                ps -> {
                    setNullableString(ps, 1, policy.toJson());
                    ps.setLong(2, table.oid());
                });
        if (updated == 0) {
            throw new CatalogException("setMaintenancePolicy found no table " + table);
        }
    }

    private static final String REQUEST_MAINTENANCE = """
            INSERT INTO tierdb.maintenance_requests (table_id, requested_by)
            VALUES (?, ?)
            ON CONFLICT (table_id) DO UPDATE
               SET requested_at = now(), requested_by = EXCLUDED.requested_by
            """;

    @Override
    public void requestMaintenance(TableId table, String requestedBy) {
        db.update(REQUEST_MAINTENANCE, ps -> {
            ps.setLong(1, table.oid());
            ps.setString(2, requestedBy);
        });
    }

    @Override
    public boolean consumeMaintenanceRequest(TableId table) {
        return db.update(
                "DELETE FROM tierdb.maintenance_requests WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid())) > 0;
    }

    @Override
    public void initCutline(TableId table, TierKey t, LakeSnapshotId snapshot,
            String lakePropsJson) {
        db.update(
                "INSERT INTO tierdb.cutline (table_id, tier_key_hi, lake_snapshot_id, lake_props) "
                        + "VALUES (?, ?, ?, ?::jsonb)",
                ps -> {
                    ps.setLong(1, table.oid());
                    ps.setLong(2, t.value());
                    ps.setLong(3, snapshot.id());
                    setNullableString(ps, 4, lakePropsJson);
                });
    }

    @Override
    public Cutline readCutline(TableId table) {
        return db.queryOne(
                "SELECT tier_key_hi, lake_snapshot_id FROM tierdb.cutline WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> new Cutline(new TierKey(rs.getLong(1)), new LakeSnapshotId(rs.getLong(2))))
                .orElseThrow(() -> new CatalogException("no cut-line for table " + table));
    }

    @Override
    public Optional<String> readLakeProps(TableId table) {
        return db.queryOne(
                "SELECT lake_props::text FROM tierdb.cutline WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> rs.getString(1));
    }

    private static final String ADVANCE_CUTLINE = """
            UPDATE tierdb.cutline
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
            UPDATE tierdb.cutline
               SET lake_props = coalesce(lake_props, '{}'::jsonb) || ?::jsonb
             WHERE table_id = ?
            """;

    @Override
    public void advanceCutline(TableId table, TierKey newT, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch) {
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
            UPDATE tierdb.cutline
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
                "SELECT replicated_lsn FROM tierdb.cutline WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> {
                    long lsn = rs.getLong(1);
                    return rs.wasNull() ? null : new Lsn(lsn);
                });
    }

    private static final String ADVANCE_FRONTIER = """
            UPDATE tierdb.cutline
               SET replicated_lsn = ?, lake_snapshot_id = ?, updated_at = now()
             WHERE table_id = ?
               AND coalesce(replicated_lsn, -9223372036854775808) <= ?
               AND lake_snapshot_id <= ?
            """;

    @Override
    public void advanceMirrorFrontier(TableId table, Lsn lsn, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch) {
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
            UPDATE tierdb.cutline
               SET lake_snapshot_id = ?, updated_at = now()
             WHERE table_id = ? AND lake_snapshot_id <= ?
            """;

    private static final String CLEAR_DELTA_ROW = """
            DELETE FROM tierdb.delta
             WHERE table_id = ? AND pk = ? AND version <= ?
            """;

    private static final String COUNT_PINS = """
            SELECT count(*) FROM tierdb.read_pins
             WHERE table_id = ? AND expires_at > now()
            """;

    @Override
    public void publishCompaction(TableId table, LakeSnapshotId snapshot, DeltaBatch folded,
            Map<String, String> lakePropsPatch) {
        db.inTransaction(c -> {
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
            UPDATE tierdb.cutline
               SET retention_line = ?, updated_at = now()
             WHERE table_id = ? AND coalesce(retention_line, -9223372036854775808) <= ?
            """;

    private static final String PURGE_DELTA_BELOW = """
            DELETE FROM tierdb.delta WHERE table_id = ? AND tier_key < ?
            """;

    private static final String DROP_EXPIRED_PARTITIONS = """
            DELETE FROM tierdb.partitions
             WHERE table_id = ? AND tier_key_hi <= ? AND state = 'dropped'
            """;

    @Override
    public void publishRetention(TableId table, LakeSnapshotId snapshot, TierKey below,
            Map<String, String> lakePropsPatch) {
        db.inTransaction(c -> {
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
                "SELECT retention_line FROM tierdb.cutline WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> {
                    long line = rs.getLong(1);
                    return rs.wasNull() ? null : new TierKey(line);
                });
    }

    private static final String READ_HORIZON = """
            SELECT min(pinned_tier_key_hi), min(pinned_lake_snapshot_id)
              FROM tierdb.read_pins WHERE table_id = ? AND expires_at > now()
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

    private static final String INSERT_LOAD_LABEL = """
            INSERT INTO tierdb.load_labels (table_id, label, state, staged_files, result)
            VALUES (?, ?, ?, ?::jsonb, ?::jsonb)
            ON CONFLICT (table_id, label) DO NOTHING
            """;

    @Override
    public boolean beginLoad(TableId table, String label, LoadState state, String stagedFilesJson,
            String resultJson) {
        return db.update(INSERT_LOAD_LABEL, ps -> {
            ps.setLong(1, table.oid());
            ps.setString(2, label);
            ps.setString(3, state.sql());
            setNullableString(ps, 4, stagedFilesJson);
            setNullableString(ps, 5, resultJson);
        }) > 0;
    }

    @Override
    public Optional<LoadLabel> lookupLoad(TableId table, String label) {
        return db.queryOne(
                "SELECT state, staged_files, result FROM tierdb.load_labels "
                        + "WHERE table_id = ? AND label = ?",
                ps -> {
                    ps.setLong(1, table.oid());
                    ps.setString(2, label);
                },
                rs -> new LoadLabel(table, label, LoadState.fromSql(rs.getString(1)),
                        rs.getString(2), rs.getString(3)));
    }

    @Override
    public List<LoadLabel> stagedLoads(TableId table) {
        return db.queryList(
                "SELECT label, staged_files, result FROM tierdb.load_labels "
                        + "WHERE table_id = ? AND state = 'staged' ORDER BY created_at, label",
                ps -> ps.setLong(1, table.oid()),
                rs -> new LoadLabel(table, rs.getString(1), LoadState.STAGED,
                        rs.getString(2), rs.getString(3)));
    }

    private static final String ADVANCE_SNAPSHOT_FLOOR = """
            UPDATE tierdb.cutline
               SET lake_snapshot_id = greatest(lake_snapshot_id, ?), updated_at = now()
             WHERE table_id = ?
            """;

    private static final String COMMIT_LABELS = """
            UPDATE tierdb.load_labels
               SET state = 'committed', updated_at = now()
             WHERE table_id = ? AND label = ? AND state = 'staged'
            """;

    @Override
    public void finishLoad(TableId table, List<String> labels, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch) {
        db.inTransaction(c -> {
            try (PreparedStatement ps = c.prepareStatement(ADVANCE_SNAPSHOT_FLOOR)) {
                ps.setLong(1, snapshot.id());
                ps.setLong(2, table.oid());
                if (ps.executeUpdate() == 0) {
                    throw new CatalogException("finishLoad found no cut-line for " + table);
                }
            }
            try (PreparedStatement ps = c.prepareStatement(COMMIT_LABELS)) {
                for (String label : labels) {
                    ps.setLong(1, table.oid());
                    ps.setString(2, label);
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

    private static final String UPSERT_OP = """
            INSERT INTO tierdb.op_log (op_id, table_id, op_kind, phase, lake_snapshot_id, details)
            VALUES (?, ?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (op_id) DO UPDATE
               SET phase = EXCLUDED.phase,
                   lake_snapshot_id = coalesce(EXCLUDED.lake_snapshot_id, tierdb.op_log.lake_snapshot_id),
                   details = coalesce(EXCLUDED.details, tierdb.op_log.details),
                   updated_at = now()
            """;

    @Override
    public void logOpPhase(UUID opId, TableId table, OpKind opKind, OpPhase phase,
            LakeSnapshotId snapshot, String detailsJson) {
        db.update(UPSERT_OP, ps -> {
            ps.setObject(1, opId);
            ps.setLong(2, table.oid());
            ps.setString(3, opKind.sql());
            ps.setString(4, phase.sql());
            if (snapshot == null) {
                ps.setNull(5, Types.BIGINT);
            } else {
                ps.setLong(5, snapshot.id());
            }
            setNullableString(ps, 6, detailsJson);
        });
    }

    @Override
    public List<TieringOp> findIncompleteOps(TableId table, OpKind opKind) {
        return db.queryList(
                "SELECT op_id, phase, lake_snapshot_id, details FROM tierdb.op_log "
                        + "WHERE table_id = ? AND op_kind = ? AND phase NOT IN (?, ?) "
                        + "ORDER BY updated_at",
                ps -> {
                    ps.setLong(1, table.oid());
                    ps.setString(2, opKind.sql());
                    ps.setString(3, OpPhase.ADVANCED.sql());
                    ps.setString(4, OpPhase.ABANDONED.sql());
                },
                rs -> {
                    long snap = rs.getLong("lake_snapshot_id");
                    boolean noSnap = rs.wasNull();
                    return new TieringOp(
                            rs.getObject("op_id", UUID.class),
                            table,
                            opKind,
                            OpPhase.fromSql(rs.getString("phase")),
                            noSnap ? Optional.empty() : Optional.of(new LakeSnapshotId(snap)),
                            rs.getString("details"));
                });
    }

    private static final String UPSERT_PARTITION = """
            INSERT INTO tierdb.partitions
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
                        + "FROM tierdb.partitions WHERE table_id = ?",
                ps -> ps.setLong(1, table.oid()),
                rs -> new PartitionInfo(
                        new PartitionId(table, rs.getString("partition_id")),
                        new PartitionBounds(
                                new TierKey(rs.getLong("tier_key_lo")),
                                new TierKey(rs.getLong("tier_key_hi"))),
                        PartitionState.valueOf(rs.getString("state").toUpperCase())));
    }

    private static final String TRANSITION = """
            UPDATE tierdb.partitions
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
                rs.getString("storage_profile"),
                TableMode.fromSql(rs.getString("mode")),
                rs.getString("publication_name"),
                rs.getString("slot_name"),
                nullableLong(rs, "heap_retention_lag"),
                nullableLong(rs, "lake_retention_lag"),
                rs.getBoolean("keep_heap"),
                MaintenancePolicy.fromJson(rs.getString("maintenance_policy")),
                TierKeyType.forType(rs.getString("tier_key_type")));
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
