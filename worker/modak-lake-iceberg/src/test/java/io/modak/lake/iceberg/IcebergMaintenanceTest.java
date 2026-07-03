package io.modak.lake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.RowBatchData;
import io.modak.common.RowBatchData.Column;
import io.modak.common.RowBatchData.ColumnType;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitter;
import io.modak.lake.LakeTieringProps;
import io.modak.lake.LakeWriter;
import io.modak.lake.MaintenanceConfig;
import io.modak.lake.MaintenanceResult;
import io.modak.lake.WriterInitContext;
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
                tables, ref, COLUMNS, Set.of("id", "ts"), "ts", 100);
    }

    private static Map<String, String> props() {
        Map<String, String> props = new HashMap<>();
        props.put(LakeTieringProps.OP_ID, UUID.randomUUID().toString());
        props.put(LakeTieringProps.OP_KIND, LakeTieringProps.OP_KIND_MAINTENANCE);
        props.put(LakeTieringProps.COMMIT_USER, LakeTieringProps.COMMIT_USER_MAINTENANCE);
        return props;
    }

    /** One small commit: a couple of rows per tier-key band -> tiny files. */
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
        commitProps.put(LakeTieringProps.OP_KIND, LakeTieringProps.OP_KIND_TIERING);
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

        MaintenanceConfig config = new MaintenanceConfig(
                1024 * 1024, 4, Long.MAX_VALUE, 100);
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(config, Long.MAX_VALUE, props());

        assertEquals(6, result.rewrittenFiles());
        assertEquals(2, result.addedFiles(), "one packed file per partition");
        assertEquals(2, dataFileCount());
        assertEquals(6, rowCount(), "bin-pack must not change the data");
    }

    @Test
    void binPackWaitsForTheMinInputThreshold() throws Exception {
        smallCommit(5, 105);
        MaintenanceConfig config = new MaintenanceConfig(
                1024 * 1024, 8, Long.MAX_VALUE, 100);
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(config, Long.MAX_VALUE, props());
        assertEquals(0, result.rewrittenFiles());
        assertEquals(2, dataFileCount());
    }

    @Test
    void expiryDropsOldSnapshotsButNeverThePinnedHorizon() throws Exception {
        smallCommit(5);
        smallCommit(10);
        smallCommit(15);
        assertEquals(3, snapshotCount());

        // Pin at sequence 1 (the first commit): nothing may expire.
        MaintenanceConfig aggressive = new MaintenanceConfig(1, 1000, 0, 1);
        MaintenanceResult gated = new IcebergMaintenance(tables.load(ref))
                .run(aggressive, 1, props());
        assertEquals(0, gated.expiredSnapshots());
        assertEquals(3, snapshotCount());

        // Pin at the latest sequence: older snapshots go, the pinned one stays.
        Table table = tables.load(ref);
        long latestSeq = table.currentSnapshot().sequenceNumber();
        MaintenanceResult expired = new IcebergMaintenance(table)
                .run(aggressive, latestSeq, props());
        assertEquals(2, expired.expiredSnapshots());
        assertEquals(1, snapshotCount());
        assertEquals(3, rowCount(), "the retained snapshot still reads fully");
    }

    @Test
    void filesWithEqualityDeletesAreLeftForTheMergePath() throws Exception {
        smallCommit(5);
        smallCommit(10);
        // A tombstone attaches an equality delete to partition 0.
        io.modak.common.DeltaRowsBatch delta = new io.modak.common.DeltaRowsBatch(
                TABLE, List.of("id"), COLUMNS, List.of(
                        new io.modak.common.DeltaRowsBatch.Entry("1", true, 5L, 1,
                                new Object[] {1L, 5L, null})));
        new IcebergMergeWriter(tables.load(ref)).applyDelta(delta, props());

        MaintenanceConfig config = new MaintenanceConfig(1024 * 1024, 2, Long.MAX_VALUE, 100);
        MaintenanceResult result = new IcebergMaintenance(tables.load(ref))
                .run(config, Long.MAX_VALUE, props());
        assertEquals(0, result.rewrittenFiles(),
                "files under equality deletes must not be rewritten");
        assertEquals(1, rowCount());
        assertTrue(result.expiredSnapshots() == 0);
    }
}
