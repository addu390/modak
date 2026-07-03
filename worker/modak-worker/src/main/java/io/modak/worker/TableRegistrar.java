package io.modak.worker;

import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.catalog.TableRegistration;
import io.modak.cdc.ReplicationSource;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.PgValues;
import io.modak.common.RowBatchData.Column;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.lake.CommittedLakeSnapshot;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeCommitter;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringProps;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;

/**
 * The onboarding command ({@code modak-worker register}): creates the cold lake
 * table via the format plugin, registers it in {@code modak.tables}, and syncs
 * partitions. Mirrored mode creates the CDC plumbing and runs a chunked,
 * journaled initial copy; re-running after a crash resumes from the journal.
 */
final class TableRegistrar {

    private static final int DEFAULT_COPY_CHUNK_ROWS = 50_000;

    private TableRegistrar() {}

    static void run(WorkerConfig config, String[] args) throws Exception {
        run(config, args, LakePlugins.load(config.lakeFormat(), config.lakeConfig()));
    }

    static void run(WorkerConfig config, String[] args, LakeStorage lake) throws Exception {
        String qualified = argOf(args, "--table");
        List<String> pks = List.of(argOf(args, "--pk").split(","));
        String tierKey = argOf(args, "--tier-key");
        TableMode mode = TableMode.fromSql(argOf(args, "--mode", "tiered"));
        String heapRetentionArg = argOf(args, "--heap-retention", null);
        Optional<Long> heapRetentionLag = heapRetentionArg == null
                ? Optional.empty()
                : Optional.of(Long.parseLong(heapRetentionArg));
        String lakeRetentionArg = argOf(args, "--lake-retention", null);
        Optional<Long> lakeRetentionLag = lakeRetentionArg == null
                ? Optional.empty()
                : Optional.of(Long.parseLong(lakeRetentionArg));
        if (lakeRetentionLag.isPresent() && mode != TableMode.TIERED) {
            throw new IllegalArgumentException("--lake-retention applies only to tiered "
                    + "tables: a mirrored heap drop relies on the lake holding full history");
        }
        String widthArg = argOf(args, "--partition-width", null);
        int chunkRows = Integer.parseInt(
                argOf(args, "--chunk-rows", Integer.toString(DEFAULT_COPY_CHUNK_ROWS)));
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

        long partitionWidth = partitionWidth(widthArg, mode, ds, qualified);
        if (partitionWidth > 0) {
            Log.info("lake layout: truncate(%s, %d)", tierKey, partitionWidth);
        }
        if (lakeRetentionLag.isPresent() && partitionWidth <= 0) {
            throw new IllegalArgumentException("--lake-retention needs a partition width "
                    + "(range partitions or --partition-width): the retention boundary must "
                    + "align to the lake's file layout");
        }

        String location = lake.tableRef(schema, table);
        String metadataLocation = lake.createTableIfAbsent(
                location, columns, required, tierKey, partitionWidth);
        Log.info("cold table ready at %s", location);

        String partitionScheme = "{\"unit\":\"range\",\"partition_width\":" + partitionWidth + "}";

        // Seed metadata_location so pinned reads work even before the first commit.
        String lakeProps = "{\"metadata_location\": \""
                + metadataLocation.replace("\"", "\\\"") + "\"}";

        if (mode == TableMode.MIRRORED) {
            registerMirrored(config, lake, ds, catalog, schema, table, pks, tierKey,
                    location, lakeProps, heapRetentionLag, partitionScheme, columns, chunkRows);
        } else {
            registerTiered(config, ds, catalog, qualified, schema, table, pks, tierKey,
                    location, lakeProps, partitionScheme, lakeRetentionLag);
        }
    }

    private static long partitionWidth(String widthArg, TableMode mode,
            DataSource ds, String qualified) {
        if (widthArg != null) {
            long width = Long.parseLong(widthArg);
            if (width < 0) {
                throw new IllegalArgumentException("--partition-width must be >= 0: " + width);
            }
            return width;
        }
        if (mode == TableMode.TIERED) {
            return io.modak.tiering.PartitionSync.firstRangeWidth(ds, qualified).orElse(0);
        }
        return 0;
    }

    private static void registerTiered(WorkerConfig config, DataSource ds, JdbcCatalog catalog,
            String qualified, String schema, String table, List<String> pks, String tierKey,
            String location, String lakeProps, String partitionScheme,
            Optional<Long> lakeRetentionLag) throws Exception {
        TableId id = catalog.register(new TableRegistration(
                relOid(ds, schema + "." + table), schema, table,
                pks, tierKey,
                partitionScheme, config.lakeFormat(), location, lakeProps,
                TableMode.TIERED, null, null, Optional.empty(), lakeRetentionLag));

        RegisteredTable registered = catalog.get(id).orElseThrow();
        int partitions = new io.modak.tiering.PartitionSync(ds, catalog).sync(registered);

        long floor = catalog.listPartitions(id).stream()
                .mapToLong(p -> p.bounds().lo().value())
                .min().orElse(0);
        catalog.initCutline(id, new TierKey(floor), new LakeSnapshotId(0));

        Log.info("registered %s (table_id=%d, %d partition(s), cutline T=%d)",
                qualified, id.oid(), partitions, floor);
    }

    private static void registerMirrored(WorkerConfig config, LakeStorage lake,
            DataSource ds, JdbcCatalog catalog,
            String schema, String table, List<String> pks, String tierKey,
            String location, String lakeProps, Optional<Long> heapRetentionLag,
            String partitionScheme, List<Column> columns, int chunkRows) throws Exception {
        String qualified = schema + "." + table;
        String publication = replicationName("modak_pub", schema, table);
        String slot = replicationName("modak_slot", schema, table);
        long oid = relOid(ds, qualified);
        TableId id = new TableId(oid);

        Optional<RegisteredTable> existing = catalog.get(id);
        Lsn resumePoint = existing.isPresent()
                ? InitialCopy.inFlightConsistentPoint(ds, id)
                : null;
        if (existing.isPresent() && resumePoint == null) {
            if (catalog.readMirrorFrontier(id).isEmpty()) {
                throw new IllegalStateException(qualified + " is stuck in a partial "
                        + "registration (no copy journal, no frontier) — run unregister, "
                        + "then register again");
            }
            throw new IllegalStateException(qualified + " is already registered");
        }

        Lsn consistentPoint;
        if (existing.isEmpty()) {
            try (Connection admin = ds.getConnection()) {
                // Re-run after a partial failure: plumbing without a catalog row is stale.
                ReplicationSource.dropSlot(admin, slot);
                ReplicationSource.dropPublication(admin, publication);
                // Deletes need the old image's tier-key; updates may need it for TOAST.
                try (Statement s = admin.createStatement()) {
                    s.execute("ALTER TABLE " + qualified + " REPLICA IDENTITY FULL");
                }
                ReplicationSource.createPublication(admin, publication, qualified);
            }
            // The slot pins WAL at the consistent point; streaming replays what the copy raced with.
            try (Connection repl = ReplicationSource.replicationConnection(
                    config.pgUrl(), config.pgUser(), config.pgPassword())) {
                ReplicationSource.SlotCreation created =
                        ReplicationSource.createSlotWithExportedSnapshot(repl, slot);
                consistentPoint = created.consistentPoint();
                Log.info("slot %s at %s", created.slotName(), consistentPoint.toPg());
            }

            catalog.register(new TableRegistration(
                    oid, schema, table, pks, tierKey,
                    partitionScheme, config.lakeFormat(), location,
                    lakeProps, TableMode.MIRRORED, publication, slot, heapRetentionLag,
                    Optional.empty()));
            // T=MIN, no frontier: reads stay on the heap until the copy lands.
            catalog.initCutline(id, new TierKey(Long.MIN_VALUE), new LakeSnapshotId(0));
            InitialCopy.begin(ds, id, consistentPoint);
        } else {
            consistentPoint = resumePoint;
            requireSlot(ds, slot);
            RegisteredTable meta = existing.get();
            pks = meta.primaryKeyCols();
            tierKey = meta.tierKeyCol();
            heapRetentionLag = meta.heapRetentionLag();
            location = meta.lakeTableRef();
            Log.info("resuming initial copy for %s from the journal", qualified);
        }

        LakeCommitResult copied = InitialCopy.run(ds, lake, id, schema, table, location,
                pks, tierKey, columns, consistentPoint, chunkRows);

        LakeSnapshotId snapshot;
        Map<String, String> publish;
        if (copied != null) {
            snapshot = copied.readable();
            publish = copied.publishProps();
        } else {
            // Crash between last chunk and finish (or empty table): adopt the lake's own snapshot.
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
            partitions = new io.modak.tiering.PartitionSync(ds, catalog)
                    .sync(catalog.get(id).orElseThrow());
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
                    catalog.readCutline(id).snapshot(), LakeTieringProps.OP_KIND_MIRROR);
        }
    }

    private static void requireSlot(DataSource ds, String slot) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?")) {
            ps.setString(1, slot);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("replication slot " + slot
                            + " is gone — the in-flight copy cannot resume without a WAL "
                            + "anchor; unregister and re-register the table");
                }
            }
        }
    }

    // publication/slot names must be sane identifiers (slots: [a-z0-9_] only).
    static String replicationName(String prefix, String schema, String table) {
        String raw = prefix + "_" + schema + "_" + table;
        return raw.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }

    static String argOf(String[] args, String flag) {
        String value = argOf(args, flag, null);
        if (value == null) {
            throw new IllegalArgumentException("missing required argument: " + flag);
        }
        return value;
    }

    static String argOf(String[] args, String flag, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return fallback;
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
