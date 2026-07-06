package io.tierdb.compaction;

import io.tierdb.common.OpPhase;
import io.tierdb.common.OpKind;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogException;
import io.tierdb.catalog.InMemoryCatalog;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.Cutline;
import io.tierdb.common.DeltaBatch;
import io.tierdb.common.DeltaRowsBatch;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.lake.ColdTableSpec;
import io.tierdb.lake.LakePartition;
import io.tierdb.lake.TierKeyWindow;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.LakeSnapshotReader;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.LakeTable;
import io.tierdb.lake.commit.LakeTieringFactory;
import io.tierdb.lake.commit.LakeTieringProps;
import io.tierdb.lake.commit.MergeWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.tierdb.lake.maintain.MaintenancePlan;
import io.tierdb.lake.maintain.MaintenanceResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompactionWorkerTest {

    private static final Instant NOW = Instant.parse("2026-07-01T00:00:00Z");

    private InMemoryCatalog catalog;
    private TableId table;
    private FakeMergeLake lake;

    /** Records folds and hands back monotonically increasing snapshots. */
    private static final class FakeMergeLake implements LakeStorage {
        final List<DeltaBatch> folds = new ArrayList<>();
        final List<Map<String, String>> stampedProps = new ArrayList<>();
        long nextSnapshot = 2;

        @Override
        public String tableRef(String schema, String table) {
            throw new UnsupportedOperationException("not needed for compaction tests");
        }

        @Override
        public String createTableIfAbsent(String ref, List<Column> columns,
                java.util.Set<String> requiredCols, String tierKeyCol,
                LakePartition partition) {
            throw new UnsupportedOperationException("not needed for compaction tests");
        }

        @Override
        public void dropTable(String ref) {
            throw new UnsupportedOperationException("not needed for compaction tests");
        }

        @Override
        public LakeTieringFactory<?, ?> tieringFactory() {
            throw new UnsupportedOperationException("not needed for compaction tests");
        }

        @Override
        public LakeSnapshotReader snapshotReader() {
            throw new UnsupportedOperationException("not needed for compaction tests");
        }

        @Override
        public LakeTable table(CommitterInitContext ctx, ColdTableSpec spec) {
            return new FakeMergeTable();
        }

        private final class FakeMergeTable implements LakeTable {
            @Override
            public MergeWriter mergeWriter() {
                return (batch, props) -> {
                    folds.add(batch);
                    stampedProps.add(Map.copyOf(props));
                    Map<String, String> publish = new HashMap<>();
                    publish.put("metadata_location",
                            "/fake/metadata/" + nextSnapshot + ".metadata.json");
                    return LakeCommitResult.committedIsReadable(
                            new LakeSnapshotId(nextSnapshot++), publish);
                };
            }

            @Override
            public void evolveSchema(List<Column> addColumns) {
                throw new UnsupportedOperationException("not needed for compaction tests");
            }

            @Override
            public MaintenanceResult maintain(MaintenancePlan plan,
                                              Map<String, String> snapshotProps) {
                return MaintenanceResult.NOOP;
            }

            @Override
            public LakeCommitResult expireBelow(long boundary,
                                                Map<String, String> snapshotProps) {
                return null;
            }

            @Override
            public LakeCommitResult ingest(List<String> files,
                                           TierKeyWindow window, Map<String, String> snapshotProps) {
                throw new UnsupportedOperationException("not needed for compaction tests");
            }

            @Override
            public List<String> stageRows(List<String> columns, Iterable<Object[]> rows) {
                throw new UnsupportedOperationException("not needed for compaction tests");
            }
        }
    }

    private static DeltaRowsBatch batchOf(TableId table, DeltaRowsBatch.Entry... entries) {
        return new DeltaRowsBatch(table, List.of("id"),
                List.of(new Column("id", ColumnType.LONG), new Column("val", ColumnType.TEXT)),
                List.of(entries));
    }

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        table = catalog.register(new TableRegistration(
                42L, "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"hour\"}", "iceberg", "/wh/events"));
        catalog.initCutline(table, new TierKey(1000), new LakeSnapshotId(1));
        lake = new FakeMergeLake();
    }

    @Test
    void oneCycleFoldsPublishesAndClears() throws Exception {
        DeltaRowsBatch batch = batchOf(table,
                new DeltaRowsBatch.Entry("3", false, 500, 7, new Object[] {3L, "fixed"}),
                new DeltaRowsBatch.Entry("4", true, 600, 8, null));
        CompactionWorker worker = new CompactionWorker(catalog, lake, (t, now) -> Optional.of(batch));

        worker.runCycle(table, NOW);

        assertEquals(1, lake.folds.size());
        assertEquals(OpKind.COMPACTION.sql(),
                lake.stampedProps.get(0).get(LakeTieringProps.OP_KIND));

        Cutline after = catalog.readCutline(table);
        assertEquals(new TierKey(1000), after.t(), "compaction never moves T");
        assertEquals(new LakeSnapshotId(2), after.snapshot());
        assertTrue(catalog.readLakeProps(table).orElseThrow().contains("2.metadata.json"));

        assertEquals(List.of(new DeltaBatch.Key("3", 7), new DeltaBatch.Key("4", 8)),
                catalog.clearedDeltaKeys());
        assertTrue(catalog.findIncompleteOps(table, OpKind.COMPACTION).isEmpty());
    }

    @Test
    void anActivePinBlocksTheWholeCycle() throws Exception {
        catalog.addReadPin(table, new Cutline(new TierKey(1000), new LakeSnapshotId(1)));
        CompactionWorker worker = new CompactionWorker(catalog, lake,
                (t, now) -> Optional.of(batchOf(table,
                        new DeltaRowsBatch.Entry("3", false, 500, 7, new Object[] {3L, "x"}))));

        worker.runCycle(table, NOW);

        assertTrue(lake.folds.isEmpty(), "no fold while a reader is pinned");
        assertEquals(new LakeSnapshotId(1), catalog.readCutline(table).snapshot());
    }

    @Test
    void aPinAcquiredAfterTheFoldBlocksPublishAndTheOpIsAbandonedNextCycle() {
        CompactionPolicy racyPolicy = (t, now) -> {
            catalog.addReadPin(table, new Cutline(new TierKey(1000), new LakeSnapshotId(1)));
            return Optional.of(batchOf(table,
                    new DeltaRowsBatch.Entry("3", false, 500, 7, new Object[] {3L, "x"})));
        };
        CompactionWorker worker = new CompactionWorker(catalog, lake, racyPolicy);

        assertThrows(CatalogException.class, () -> worker.runCycle(table, NOW));

        assertEquals(1, lake.folds.size(), "the fold itself committed to the lake");
        assertEquals(new LakeSnapshotId(1), catalog.readCutline(table).snapshot(),
                "but nothing was published");
        assertTrue(catalog.clearedDeltaKeys().isEmpty(), "and no delta rows were cleared");

        assertEquals(1, catalog.findIncompleteOps(table, OpKind.COMPACTION).size());

        CompactionWorker idle = new CompactionWorker(catalog, lake, (t, now) -> Optional.empty());
        try {
            idle.runCycle(table, NOW);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertTrue(catalog.findIncompleteOps(table, OpKind.COMPACTION).isEmpty());
    }

    @Test
    void emptySelectionDoesNothing() throws Exception {
        CompactionWorker worker = new CompactionWorker(catalog, lake, (t, now) -> Optional.empty());
        worker.runCycle(table, NOW);
        assertTrue(lake.folds.isEmpty());
        assertEquals(new LakeSnapshotId(1), catalog.readCutline(table).snapshot());
    }

    @Test
    void staleIncompleteOpsAreAbandonedAtCycleStart() throws Exception {
        UUID crashed = UUID.randomUUID();
        catalog.logOpPhase(crashed, table, OpKind.COMPACTION, OpPhase.FLUSHING,
                null, null);

        CompactionWorker worker = new CompactionWorker(catalog, lake, (t, now) -> Optional.empty());
        worker.runCycle(table, NOW);

        assertTrue(catalog.findIncompleteOps(table, OpKind.COMPACTION).isEmpty());
    }
}
