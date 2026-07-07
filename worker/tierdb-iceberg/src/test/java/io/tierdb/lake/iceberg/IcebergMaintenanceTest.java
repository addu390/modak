package io.tierdb.lake.iceberg;

import io.tierdb.common.OpKind;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.lake.LakePartition;
import io.tierdb.common.DeltaRowsBatch;
import io.tierdb.common.RowBatchData;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.lake.iceberg.commit.IcebergCommittable;
import io.tierdb.lake.iceberg.commit.IcebergMergeWriter;
import io.tierdb.lake.iceberg.commit.IcebergTieringFactory;
import io.tierdb.lake.iceberg.commit.IcebergWriteResult;
import io.tierdb.lake.iceberg.maintain.IcebergMaintenance;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeCommitter;
import io.tierdb.lake.commit.LakeTieringProps;
import io.tierdb.lake.commit.LakeWriter;
import io.tierdb.lake.maintain.MaintenancePlan;
import io.tierdb.lake.maintain.MaintenanceResult;
import io.tierdb.lake.commit.WriterInitContext;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergMaintenanceTest {

    private static final TableId TABLE = new TableId(42);
    private static final List<Column> COLUMNS = List.of(
            new Column("id", ColumnType.LONG),
            new Column("ts", ColumnType.LONG),
            new Column("val", ColumnType.TEXT));

    @TempDir
    Path tmp;

    private IcebergTables tables;
    private String ref;
    private long nextId = 1;

    @BeforeEach
    void setUp() {
        tables = IcebergTables.from(Map.of(), new Configuration());
        ref = tmp.resolve("public.events").toString();
        IcebergTableBootstrap.createIfAbsent(
                tables, ref, COLUMNS, Set.of("id", "ts"), "ts", LakePartition.truncate(100));
    }

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(LakeTieringProps.OP_ID, UUID.randomUUID().toString());
        props.put(LakeTieringProps.OP_KIND, OpKind.MAINTENANCE.sql());
        props.put(LakeTieringProps.COMMIT_USER, OpKind.MAINTENANCE.commitUser());
        return props;
    }

    private static MaintenancePlan plan(Map<String, String> settings, long pinnedFloor) {
        return new MaintenancePlan(settings, pinnedFloor, List.of());
    }

    private static Map<String, String> inert() {
        Map<String, String> settings = new HashMap<>();
        settings.put("rewrite_target_bytes", "1");
        settings.put("rewrite_min_input_files", "1000");
        settings.put("snapshot_retention_hours", String.valueOf(Long.MAX_VALUE / 3_600_000L));
        settings.put("snapshot_min_retained", "100");
        settings.put("delete_compaction_min_deletes", "1000");
        settings.put("manifest_rewrite_min_manifests", "100000");
        return settings;
    }

    private void smallCommit(long... tierKeys) throws Exception {
        IcebergTieringFactory factory = new IcebergTieringFactory(tables);
        PartitionId pid = new PartitionId(TABLE, "p_" + nextId);
        PartitionBounds bounds = new PartitionBounds(new TierKey(0), new TierKey(1000));
        List<Object[]> rows = new ArrayList<>();
        for (long ts : tierKeys) {
            rows.add(new Object[] {nextId++, ts, "v"});
        }
        IcebergWriteResult result;
        try (LakeWriter<IcebergWriteResult> writer =
                factory.createWriter(new WriterInitContext(TABLE, pid, bounds, ref))) {
            writer.write(new RowBatchData(pid, bounds, COLUMNS, rows));
            result = writer.complete();
        }
        Map<String, String> commitProps = props();
        commitProps.put(LakeTieringProps.OP_KIND, OpKind.TIERING.sql());
        try (LakeCommitter<IcebergWriteResult, IcebergCommittable> committer =
                factory.createCommitter(new CommitterInitContext(TABLE, ref))) {
            committer.commit(committer.toCommittable(List.of(result)), commitProps);
        }
    }

    private int dataFileCount() throws Exception {
        Table table = tables.load(ref);
        int count = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask ignored : tasks) {
                count++;
            }
        }
        return count;
    }

    private long rowCount() throws Exception {
        Table table = tables.load(ref);
        long count = 0;
        try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
            for (Record ignored : it) {
                count++;
            }
        }
        return count;
    }

    private int snapshotCount() {
        int count = 0;
        for (Snapshot ignored : tables.load(ref).snapshots()) {
            count++;
        }
        return count;
    }

    @Test
    void binPackMergesSmallFilesPerPartitionWithoutChangingRows() throws Exception {
        smallCommit(5, 105);
        smallCommit(10, 110);
        smallCommit(15, 115);
        assertEquals(6, dataFileCount(), "3 commits x 2 bands");

        Map<String, String> settings = inert();
        settings.put("rewrite_target_bytes", String.valueOf(1024 * 1024));
        settings.put("rewrite_min_input_files", "4");
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(plan(settings, Long.MAX_VALUE), props());

        assertEquals(6, result.counter("rewritten_files"));
        assertEquals(2, result.counter("added_files"), "one packed file per partition");
        assertEquals(2, dataFileCount());
        assertEquals(6, rowCount(), "bin-pack must not change the data");
    }

    @Test
    void binPackWaitsForTheMinInputThreshold() throws Exception {
        smallCommit(5, 105);
        Map<String, String> settings = inert();
        settings.put("rewrite_target_bytes", String.valueOf(1024 * 1024));
        settings.put("rewrite_min_input_files", "8");
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(plan(settings, Long.MAX_VALUE), props());
        assertEquals(0, result.counter("rewritten_files"));
        assertEquals(2, dataFileCount());
    }

    @Test
    void expiryDropsOldSnapshotsButNeverThePinnedHorizon() throws Exception {
        smallCommit(5);
        smallCommit(10);
        smallCommit(15);
        assertEquals(3, snapshotCount());

        Map<String, String> aggressive = inert();
        aggressive.put("snapshot_retention_hours", "0");
        aggressive.put("snapshot_min_retained", "1");

        MaintenanceResult gated = new IcebergMaintenance(tables.load(ref))
                .run(plan(aggressive, 1), props());
        assertEquals(0, gated.counter("expired_snapshots"));
        assertEquals(3, snapshotCount());

        Table table = tables.load(ref);
        long latestSeq = table.currentSnapshot().sequenceNumber();
        MaintenanceResult expired = new IcebergMaintenance(table)
                .run(plan(aggressive, latestSeq), props());
        assertEquals(2, expired.counter("expired_snapshots"));
        assertEquals(1, snapshotCount());
        assertEquals(3, rowCount(), "the retained snapshot still reads fully");
    }

    @Test
    void filesWithEqualityDeletesAreLeftForTheMergePath() throws Exception {
        smallCommit(5);
        smallCommit(10);
        tombstone(1L, 5L);

        Map<String, String> settings = inert();
        settings.put("rewrite_target_bytes", String.valueOf(1024 * 1024));
        settings.put("rewrite_min_input_files", "2");
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(plan(settings, Long.MAX_VALUE), props());
        assertEquals(0, result.counter("rewritten_files"),
                "files under equality deletes must not be bin-packed");
        assertEquals(1, rowCount());
        assertTrue(result.counter("expired_snapshots") == 0);
    }

    @Test
    void deleteCompactionAppliesDeletesAndDropsDeleteFiles() throws Exception {
        smallCommit(5, 105);
        smallCommit(10);
        tombstone(1L, 5L);
        assertEquals(1, deleteFileCount());
        assertEquals(2, rowCount());

        Map<String, String> settings = inert();
        settings.put("delete_compaction_min_deletes", "1");
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(plan(settings, Long.MAX_VALUE), props());

        assertEquals(1, result.counter("delete_compacted_files"));
        assertEquals(1, result.counter("removed_delete_files"));
        assertEquals(0, deleteFileCount(), "the delete debt is gone");
        assertEquals(2, rowCount(), "surviving rows are intact");
        assertEquals(2, dataFileCount(), "the fully-deleted file has no replacement");
    }

    @Test
    void deleteCompactionDroppingEveryRowLeavesNoReplacementFile() throws Exception {
        smallCommit(5);
        tombstone(1L, 5L);
        assertEquals(0, rowCount(), "the tombstone removed the file's only row");

        Map<String, String> settings = inert();
        settings.put("delete_compaction_min_deletes", "1");
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(plan(settings, Long.MAX_VALUE), props());

        assertEquals(1, result.counter("delete_compacted_files"));
        assertEquals(0, deleteFileCount());
        assertEquals(0, rowCount());
        assertEquals(0, dataFileCount());
    }

    @Test
    void manifestRewriteFoldsTheManifestList() throws Exception {
        smallCommit(5);
        smallCommit(10);
        smallCommit(15);

        Map<String, String> settings = inert();
        settings.put("manifest_rewrite_min_manifests", "2");
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(plan(settings, Long.MAX_VALUE), props());

        assertTrue(result.counter("rewritten_manifests") >= 2);
        Table table = tables.load(ref);
        assertEquals(1, table.currentSnapshot().allManifests(table.io()).size());
        assertEquals(3, rowCount());
    }

    @Test
    void orphanSweepDeletesUnreferencedFilesButNeverProtectedOnes() throws Exception {
        smallCommit(5);
        Table table = tables.load(ref);
        String dataDir = table.location() + "/data";
        java.nio.file.Path orphan = Path.of(dataDir.replace("file:", ""), "orphan.parquet");
        java.nio.file.Path staged = Path.of(dataDir.replace("file:", ""), "staged.parquet");
        java.nio.file.Files.createDirectories(orphan.getParent());
        java.nio.file.Files.writeString(orphan, "never committed");
        java.nio.file.Files.writeString(staged, "awaiting adoption");

        Map<String, String> settings = inert();
        settings.put("orphan_sweep_enabled", "true");
        settings.put("orphan_grace_hours", "0");
        MaintenanceResult result = new IcebergMaintenance(table).run(
                new MaintenancePlan(settings, Long.MAX_VALUE,
                        List.of(staged.toString())),
                props());

        assertEquals(1, result.counter("orphan_files_deleted"));
        assertTrue(java.nio.file.Files.notExists(orphan), "the orphan is gone");
        assertTrue(java.nio.file.Files.exists(staged), "protected files survive");
        assertEquals(1, rowCount(), "committed data is untouched");
    }

    @Test
    void disabledPassesRunNothingEvenWithAggressiveThresholds() throws Exception {
        smallCommit(5);
        smallCommit(10);
        tombstone(1L, 5L);
        int filesBefore = dataFileCount();
        int snapshotsBefore = snapshotCount();

        Map<String, String> settings = new HashMap<>();
        settings.put("rewrite_enabled", "false");
        settings.put("rewrite_target_bytes", String.valueOf(1024 * 1024));
        settings.put("rewrite_min_input_files", "1");
        settings.put("delete_compaction_enabled", "false");
        settings.put("delete_compaction_min_deletes", "1");
        settings.put("manifest_rewrite_enabled", "false");
        settings.put("manifest_rewrite_min_manifests", "1");
        settings.put("snapshot_expiry_enabled", "false");
        settings.put("snapshot_retention_hours", "0");
        settings.put("snapshot_min_retained", "1");
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(plan(settings, Long.MAX_VALUE), props());

        assertTrue(result.isNoop());
        assertEquals(filesBefore, dataFileCount());
        assertEquals(snapshotsBefore, snapshotCount());
        assertEquals(1, deleteFileCount(), "the delete debt is untouched");
    }

    @Test
    void orphanSweepIsOffByDefault() throws Exception {
        smallCommit(5);
        Table table = tables.load(ref);
        java.nio.file.Path orphan = Path.of(
                table.location().replace("file:", ""), "data", "orphan.parquet");
        java.nio.file.Files.createDirectories(orphan.getParent());
        java.nio.file.Files.writeString(orphan, "never committed");

        MaintenanceResult result = new IcebergMaintenance(table)
                .run(plan(inert(), Long.MAX_VALUE), props());

        assertEquals(0, result.counter("orphan_files_deleted"));
        assertTrue(java.nio.file.Files.exists(orphan));
    }

    private void tombstone(long id, long tierKey) throws Exception {
        DeltaRowsBatch delta = new DeltaRowsBatch(
                TABLE, List.of("id"), COLUMNS, List.of(
                        new DeltaRowsBatch.Entry(String.valueOf(id), true,
                                tierKey, 1, new Object[] {id, tierKey, null})));
        new IcebergMergeWriter(tables.load(ref)).applyDelta(delta, props());
    }

    private int deleteFileCount() throws Exception {
        Table table = tables.load(ref);
        java.util.Set<String> paths = new java.util.HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                task.deletes().forEach(d -> paths.add(d.path().toString()));
            }
        }
        return paths.size();
    }
}
