package io.tierdb.worker.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.StorageProfile;
import io.tierdb.catalog.TableMode;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.cdc.ReplicationSource;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.Lsn;
import io.tierdb.common.OpKind;
import io.tierdb.common.PgValues;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.common.TierKeyType;
import io.tierdb.lake.LakePartition;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.commit.CommittedLakeSnapshot;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.commit.LakeCommitter;
import io.tierdb.worker.LakeStorages;
import io.tierdb.worker.Log;
import io.tierdb.worker.WorkerConfig;
import io.tierdb.tiering.PartitionSync;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;

/**
 * The onboarding command ({@code tierdb-worker register}). Creates the cold
 * lake table via the format plugin, registers it in {@code tierdb.tables},
 * and syncs partitions.
 */
public final class TableRegistrar {

    private static final int DEFAULT_COPY_CHUNK_ROWS = 50_000;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TableRegistrar() {}

    public static void run(WorkerConfig config, String[] args) throws Exception {
        String profileName = new Args(args)
                .optional("--profile", TableRegistration.DEFAULT_PROFILE);
        JdbcCatalog catalog = new JdbcCatalog(config.dataSource());
        LakeStorages storages = new LakeStorages(config, catalog);
        StorageProfile profile = storages.profile(profileName);
        run(config, args, storages.forProfile(profile),
                profileName, storages.formatOf(profile));
    }

    public static void run(WorkerConfig config, String[] args, LakeStorage lake) throws Exception {
        run(config, args, lake, TableRegistration.DEFAULT_PROFILE, config.lakeFormat());
    }

    public static void run(WorkerConfig config, String[] args, LakeStorage lake,
            String profileName, String lakeFormat) throws Exception {
        Args parsed = new Args(args);
        String qualified = parsed.required("--table");
        List<String> pks = List.of(parsed.required("--pk").split(","));
        String tierKey = parsed.required("--tier-key");
        TableMode mode = TableMode.fromSql(parsed.optional("--mode", "tiered"));
        String heapRetentionArg = parsed.optional("--heap-retention", null);
        String lakeRetentionArg = parsed.optional("--lake-retention", null);
        String lakePartitionArg = parsed.optional("--lake-partition", null);

        if (lakeRetentionArg != null && !mode.tierSplitting()) {
            throw new IllegalArgumentException("--lake-retention applies only to tiered and "
                    + "direct tables: a mirrored heap drop relies on the lake holding full history");
        }

        boolean keepHeap = parsed.has("--keep-heap");
        if (keepHeap && !mode.tierSplitting()) {
            throw new IllegalArgumentException("--keep-heap applies only to tiered and direct "
                    + "tables: a mirrored heap is already kept unless --heap-retention says otherwise");
        }
        if (keepHeap && lakeRetentionArg != null) {
            throw new IllegalArgumentException("--keep-heap and --lake-retention exclude "
                    + "each other: keep-heap means nothing is deleted anywhere");
        }

        String widthArg = parsed.optional("--partition-width", null);
        int chunkRows = Integer.parseInt(parsed.optional("--chunk-rows",
                Integer.toString(DEFAULT_COPY_CHUNK_ROWS)));

        String[] parts = qualified.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("--table must be schema-qualified: " + qualified);
        }
        String schema = parts[0];
        String table = parts[1];

        DataSource ds = config.dataSource();
        JdbcCatalog catalog = new JdbcCatalog(ds);

        List<Column> columns = columnsOf(ds, schema, table);
        Set<String> required = new HashSet<>(pks);
        required.add(tierKey);
        for (String col : required) {
            if (columns.stream().noneMatch(c -> c.name().equals(col))) {
                throw new IllegalArgumentException(
                        "pk/tier-key column '" + col + "' not found on " + qualified + ": " + columns);
            }
        }

        TierKeyType tierKeyType = tierKeyTypeOf(ds, schema, table, tierKey);
        Optional<Long> heapRetentionLag = heapRetentionArg == null
                ? Optional.empty()
                : Optional.of(tierKeyType.parseLagOrWidth(heapRetentionArg));
        Optional<Long> lakeRetentionLag = lakeRetentionArg == null
                ? Optional.empty()
                : Optional.of(tierKeyType.parseLagOrWidth(lakeRetentionArg));

        long partitionWidth = partitionWidth(widthArg, mode, ds, qualified, tierKeyType);
        LakePartition lakePartition = lakePartition(lakePartitionArg, tierKeyType, partitionWidth);
        if (!lakePartition.isNone()) {
            Log.info("lake layout: %s(%s)%s", lakePartition.transform(), tierKey,
                    lakePartition.isTruncate() ? " width " + partitionWidth : "");
        }
        if (lakeRetentionLag.isPresent() && partitionWidth <= 0) {
            throw new IllegalArgumentException("--lake-retention needs a partition width "
                    + "(range partitions or --partition-width): the retention boundary must "
                    + "align to the lake's file layout");
        }

        String location = lake.tableRef(schema, table);
        Map<String, String> publishProps = lake.createTableIfAbsent(
                location, columns, required, tierKey, lakePartition);
        Log.info("cold table ready at %s", location);

        String partitionScheme = "{\"unit\":\"range\",\"partition_width\":" + partitionWidth
                + ",\"lake_transform\":\"" + lakePartition.transform() + "\"}";

        String lakeProps = MAPPER.writeValueAsString(publishProps);

        if (mode == TableMode.MIRRORED) {
            registerMirrored(config, lake, ds, catalog, schema, table, pks, tierKey, tierKeyType,
                    location, lakeProps, heapRetentionLag, partitionScheme, columns,
                    chunkRows, profileName, lakeFormat);
        } else {
            if (mode == TableMode.DIRECT) {
                requireLiveCatalog(catalog, profileName);
            }
            registerTierSplitting(ds, catalog, qualified, schema, table, pks, tierKey,
                    tierKeyType, location, lakeProps, partitionScheme, lakeRetentionLag,
                    keepHeap, mode, profileName, lakeFormat);
        }
    }

    private static void requireLiveCatalog(JdbcCatalog catalog, String profileName) {
        StorageProfile profile = catalog.storageProfile(profileName)
                .orElseThrow(() -> new IllegalArgumentException(
                        "storage profile '" + profileName + "' is not registered"));
        String config = profile.lakeConfigJson();
        boolean hasEndpoint = config != null && config.contains("catalog.uri")
                && !profile.warehouse().isBlank();
        if (!hasEndpoint) {
            throw new IllegalArgumentException("--mode direct needs a live catalog: set "
                    + "lake_config->>'catalog.uri' and a warehouse on storage profile '"
                    + profileName + "'");
        }
    }

    private static long partitionWidth(String widthArg, TableMode mode,
            DataSource ds, String qualified, TierKeyType tierKeyType) {
        if (widthArg != null) {
            long width = tierKeyType.parseLagOrWidth(widthArg);
            if (width < 0) {
                throw new IllegalArgumentException("--partition-width must be >= 0: " + widthArg);
            }
            return width;
        }
        if (mode.tierSplitting()) {
            return PartitionSync.firstRangeWidth(ds, qualified, tierKeyType)
                    .orElse(0);
        }
        return 0;
    }

    private static LakePartition lakePartition(String arg, TierKeyType tierKeyType,
            long partitionWidth) {
        boolean temporal = tierKeyType.columnType() == ColumnType.TIMESTAMP
                || tierKeyType.columnType() == ColumnType.DATE;
        if (arg != null) {
            String t = arg.toLowerCase(java.util.Locale.ROOT);
            if (t.equals("none")) {
                return LakePartition.none();
            }
            if (!temporal) {
                throw new IllegalArgumentException("--lake-partition " + arg
                        + " needs a temporal tier key, " + tierKeyType.sql()
                        + " keys are laid out with truncate via --partition-width");
            }
            if (t.equals("hour") && tierKeyType == TierKeyType.DATE) {
                throw new IllegalArgumentException(
                        "--lake-partition hour is finer than a date tier key");
            }
            return LakePartition.temporal(t);
        }
        if (temporal) {
            return LakePartition.temporal("day");
        }
        return LakePartition.truncate(partitionWidth);
    }

    private static TierKeyType tierKeyTypeOf(DataSource ds, String schema, String table,
            String column) throws Exception {
        String sql = """
                SELECT data_type FROM information_schema.columns
                 WHERE table_schema = ? AND table_name = ? AND column_name = ?
                """;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            ps.setString(3, column);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalArgumentException(
                            "tier key column not found: " + schema + "." + table + "." + column);
                }
                return TierKeyType.forType(rs.getString(1));
            }
        }
    }

    private static void registerTierSplitting(DataSource ds, JdbcCatalog catalog,
            String qualified, String schema, String table, List<String> pks, String tierKey,
            TierKeyType tierKeyType, String location, String lakeProps, String partitionScheme,
            Optional<Long> lakeRetentionLag, boolean keepHeap, TableMode mode,
            String profileName, String lakeFormat) throws Exception {
        TableId id = catalog.register(new TableRegistration(
                relOid(ds, schema + "." + table), schema, table,
                pks, tierKey,
                partitionScheme, lakeFormat, location,
                mode, null, null, Optional.empty(), lakeRetentionLag, keepHeap,
                profileName, tierKeyType));

        RegisteredTable registered = catalog.get(id).orElseThrow();
        int partitions = new PartitionSync(ds, catalog).sync(registered);

        long floor = catalog.listPartitions(id).stream()
                .mapToLong(p -> p.bounds().lo().value())
                .min().orElse(0);
        catalog.initCutline(id, new TierKey(floor), new LakeSnapshotId(0), lakeProps);
        enableTransparentWrites(ds, id);

        Log.info("registered %s (%s, table_id=%d, %d partition(s), cutline T=%d)",
                qualified, mode.sql(), id.oid(), partitions, floor);
    }

    private static void enableTransparentWrites(DataSource ds, TableId id) {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT to_regprocedure('tierdb_enable_transparent_writes(oid)') IS NOT NULL")) {
                if (!rs.next() || !rs.getBoolean(1)) {
                    return;
                }
            }
            try (ResultSet rs = s.executeQuery(
                    "SELECT tierdb_enable_transparent_writes(" + id.oid() + "::oid)")) {
                rs.next();
                Log.info("%s", rs.getString(1));
            }
        } catch (SQLException e) {
            Log.info("transparent writes not enabled: %s", e.getMessage());
        }
    }

    private static void registerMirrored(WorkerConfig config, LakeStorage lake,
            DataSource ds, JdbcCatalog catalog,
            String schema, String table, List<String> pks, String tierKey, TierKeyType tierKeyType,
            String location, String lakeProps, Optional<Long> heapRetentionLag,
            String partitionScheme, List<Column> columns, int chunkRows,
            String profileName, String lakeFormat) throws Exception {
        String qualified = schema + "." + table;
        String publication = replicationName("tierdb_pub", schema, table);
        String slot = replicationName("tierdb_slot", schema, table);
        long oid = relOid(ds, qualified);
        TableId id = new TableId(oid);

        Optional<RegisteredTable> existing = catalog.get(id);
        Lsn resumePoint = existing.isPresent()
                ? InitialCopy.inFlightConsistentPoint(ds, id)
                : null;
        if (existing.isPresent() && resumePoint == null) {
            if (catalog.readMirrorFrontier(id).isEmpty()) {
                throw new IllegalStateException(qualified + " is stuck in a partial "
                        + "registration (no copy journal, no frontier), run unregister, "
                        + "then register again");
            }
            throw new IllegalStateException(qualified + " is already registered");
        }

        Lsn consistentPoint;
        if (existing.isEmpty()) {
            try (Connection admin = ds.getConnection()) {
                ReplicationSource.dropSlot(admin, slot);
                ReplicationSource.dropPublication(admin, publication);
                try (Statement s = admin.createStatement()) {
                    s.execute("ALTER TABLE " + qualified + " REPLICA IDENTITY FULL");
                }
                ReplicationSource.createPublication(admin, publication, qualified);
            }
            try (Connection repl = ReplicationSource.replicationConnection(
                    config.pgUrl(), config.pgUser(), config.pgPassword())) {
                ReplicationSource.SlotCreation created =
                        ReplicationSource.createSlotWithExportedSnapshot(repl, slot);
                consistentPoint = created.consistentPoint();
                Log.info("slot %s at %s", created.slotName(), consistentPoint.toPg());
            }

            catalog.register(new TableRegistration(
                    oid, schema, table, pks, tierKey,
                    partitionScheme, lakeFormat, location,
                    TableMode.MIRRORED, publication, slot, heapRetentionLag,
                    Optional.empty(), false, profileName, tierKeyType));
            catalog.initCutline(id, new TierKey(Long.MIN_VALUE), new LakeSnapshotId(0), lakeProps);
            InitialCopy.begin(ds, id, consistentPoint);
        } else {
            consistentPoint = resumePoint;
            requireSlot(ds, slot);
            RegisteredTable meta = existing.get();
            pks = meta.primaryKeyCols();
            tierKey = meta.tierKeyCol();
            tierKeyType = meta.tierKeyType();
            heapRetentionLag = meta.heapRetentionLag();
            location = meta.lakeTableRef();
            Log.info("resuming initial copy for %s from the journal", qualified);
        }

        LakeCommitResult copied = InitialCopy.run(ds, lake, id, schema, table, location,
                pks, tierKey, tierKeyType, columns, consistentPoint, chunkRows);

        LakeSnapshotId snapshot;
        Map<String, String> publish;
        if (copied != null) {
            snapshot = copied.readable();
            publish = copied.publishProps();
        } else {
            Optional<CommittedLakeSnapshot> inLake =
                    probeMirrorSnapshot(lake, id, location, catalog);
            snapshot = inLake.map(CommittedLakeSnapshot::readable)
                    .orElse(catalog.readCutline(id).snapshot());
            publish = inLake.map(CommittedLakeSnapshot::publishProps).orElse(Map.of());
        }
        catalog.advanceMirrorFrontier(id, consistentPoint, snapshot, publish);
        InitialCopy.finish(ds, id);

        int partitions = 0;
        if (heapRetentionLag.isPresent()) {
            partitions = new PartitionSync(ds, catalog)
                    .sync(catalog.get(id).orElseThrow());
            enableTransparentWrites(ds, id);
        }
        Log.info("registered %s mirrored (table_id=%d, frontier=%s, initial copy %s, "
                        + "%d partition(s))",
                qualified, id.oid(), consistentPoint.toPg(),
                copied == null ? "adopted" : "committed", partitions);
    }

    private static Optional<CommittedLakeSnapshot> probeMirrorSnapshot(LakeStorage lake,
            TableId id, String location, JdbcCatalog catalog) throws Exception {
        try (LakeCommitter<?, ?> committer = lake.tieringFactory()
                .createCommitter(new CommitterInitContext(id, location))) {
            return committer.getMissingLakeSnapshot(
                    catalog.readCutline(id).snapshot(), OpKind.MIRROR);
        }
    }

    private static void requireSlot(DataSource ds, String slot) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slot);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("replication slot " + slot
                            + " is gone and the in-flight copy cannot resume without a WAL "
                            + "anchor, unregister and re-register the table");
                }
            }
        }
    }

    static String replicationName(String prefix, String schema, String table) {
        String raw = prefix + "_" + schema + "_" + table;
        return raw.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    static List<Column> columnsOf(DataSource ds, String schema, String table)
            throws Exception {
        String sql = """
                SELECT column_name, data_type,
                       COALESCE(numeric_precision, 0), COALESCE(numeric_scale, 0)
                  FROM information_schema.columns
                 WHERE table_schema = ? AND table_name = ?
                 ORDER BY ordinal_position
                """;
        List<Column> out = new ArrayList<>();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, schema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(PgValues.column(rs.getString(1), rs.getString(2),
                            rs.getInt(3), rs.getInt(4)));
                }
            }
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("no such relation: " + schema + "." + table);
        }
        return out;
    }

    private static long relOid(DataSource ds, String qualified) throws Exception {
        try (Connection c = ds.getConnection(); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(
                        "SELECT '" + qualified.replace("'", "''") + "'::regclass::oid::bigint")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
