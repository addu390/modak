package io.tierdb.lake.iceberg;

import io.tierdb.common.OpKind;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.common.DeltaRowsBatch;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.lake.LakePartition;
import io.tierdb.common.RowBatchData;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.lake.commit.LakeCommitter;
import io.tierdb.lake.commit.LakeTieringProps;
import io.tierdb.lake.commit.LakeWriter;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.WriterInitContext;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round-trip for the partitioned (tier-key truncate) layout: bootstrap creates the
 * spec, the tiering writer fans data files out per partition, and the merge writer
 * keeps equality deletes partition-scoped.
 */
class IcebergPartitionedWriteTest {

    private static final TableId TABLE = new TableId(42);
    private static final List<Column> COLUMNS = List.of(
            new Column("id", ColumnType.LONG),
            new Column("ts", ColumnType.LONG),
            new Column("val", ColumnType.TEXT));

    @TempDir
    Path tmp;

    private IcebergTables tables;
    private String ref;

    @BeforeEach
    void setUp() {
        tables = IcebergTables.from(Map.of(), new Configuration());
        ref = tmp.resolve("public.events").toString();
        IcebergTableBootstrap.createIfAbsent(
                tables, ref, COLUMNS, Set.of("id", "ts"), "ts", LakePartition.truncate(100));
    }

    private static Map<String, String> props(String kind) {
        Map<String, String> props = new HashMap<>();
        props.put(LakeTieringProps.OP_ID, UUID.randomUUID().toString());
        props.put(LakeTieringProps.OP_KIND, kind);
        props.put(LakeTieringProps.TABLE_ID, "42");
        return props;
    }

    private void tier(List<Object[]> rows, long lo, long hi) throws Exception {
        IcebergTieringFactory factory = new IcebergTieringFactory(tables);
        PartitionId pid = new PartitionId(TABLE, "p_" + lo);
        PartitionBounds bounds = new PartitionBounds(new TierKey(lo), new TierKey(hi));
        IcebergWriteResult result;
        try (LakeWriter<IcebergWriteResult> writer =
                factory.createWriter(new WriterInitContext(TABLE, pid, bounds, ref))) {
            writer.write(new RowBatchData(pid, bounds, COLUMNS, rows));
            result = writer.complete();
        }
        try (LakeCommitter<IcebergWriteResult, IcebergCommittable> committer =
                factory.createCommitter(new CommitterInitContext(TABLE, ref))) {
            committer.commit(committer.toCommittable(List.of(result)),
                    props(OpKind.TIERING.sql()));
        }
    }

    private List<Record> scan() throws Exception {
        Table table = tables.load(ref);
        List<Record> out = new ArrayList<>();
        try (CloseableIterable<Record> it = IcebergGenerics.read(table).build()) {
            it.forEach(out::add);
        }
        return out;
    }

    @Test
    void tieringFansOutOneDataFilePerTierKeyBand() throws Exception {
        tier(List.of(
                new Object[] {1L, 5L, "a"},
                new Object[] {2L, 105L, "b"},
                new Object[] {3L, 205L, "c"},
                new Object[] {4L, 110L, "d"}), 0, 300);

        Table table = tables.load(ref);
        assertTrue(table.spec().isPartitioned());
        Set<String> partitions = new HashSet<>();
        int fileCount = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                fileCount++;
                partitions.add(task.file().partition().toString());
            }
        }
        assertEquals(3, fileCount, "one data file per touched tier-key band");
        assertEquals(3, partitions.size());
        assertEquals(4, scan().size());
    }

    @Test
    void mergeKeepsDeletesPartitionScopedAndAppliesNewestWins() throws Exception {
        tier(List.of(
                new Object[] {1L, 5L, "a"},
                new Object[] {2L, 105L, "b"}), 0, 200);

        DeltaRowsBatch delta = new DeltaRowsBatch(TABLE, List.of("id"), COLUMNS, List.of(
                new DeltaRowsBatch.Entry("1", false, 5L, 1,
                        new Object[] {1L, 5L, "a-updated"}),
                new DeltaRowsBatch.Entry("2", true, 105L, 2,
                        new Object[] {2L, 105L, null})));
        Table table = tables.load(ref);
        new IcebergMergeWriter(table).applyDelta(delta, props(OpKind.MIRROR.sql()));

        List<Record> rows = scan();
        assertEquals(1, rows.size(), "id=2 tombstoned, id=1 replaced");
        assertEquals("a-updated", rows.get(0).getField("val"));

        table.refresh();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                for (var delete : task.deletes()) {
                    assertTrue(delete.partition().size() > 0,
                            "equality deletes must carry a partition on a partitioned table");
                }
            }
        }
    }

    @Test
    void aCrossTierMoveDeletesTheOldPartitionsImage() throws Exception {
        tier(List.of(
                new Object[] {1L, 5L, "a"},
                new Object[] {2L, 105L, "b"}), 0, 200);

        DeltaRowsBatch delta = new DeltaRowsBatch(TABLE, List.of("id"), COLUMNS, List.of(
                new DeltaRowsBatch.Entry("1", false, 105L, 5L, 1,
                        new Object[] {1L, 105L, "moved"})));
        new IcebergMergeWriter(tables.load(ref))
                .applyDelta(delta, props(OpKind.COMPACTION.sql()));

        List<Record> rows = scan();
        assertEquals(2, rows.size(), "no stranded image in the old partition");
        for (Record r : rows) {
            if ((Long) r.getField("id") == 1L) {
                assertEquals(105L, r.getField("ts"));
                assertEquals("moved", r.getField("val"));
            }
        }
    }

    @Test
    void unpartitionedTablesKeepTheSingleWriterLayout() throws Exception {
        String flatRef = tmp.resolve("public.flat").toString();
        IcebergTableBootstrap.createIfAbsent(
                tables, flatRef, COLUMNS, Set.of("id", "ts"), "ts", LakePartition.none());
        assertTrue(tables.load(flatRef).spec().isUnpartitioned());
    }
}
