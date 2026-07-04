package io.modak.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.common.Cutline;
import io.modak.common.DeltaBatch;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryCatalogTest {

    private InMemoryCatalog catalog;
    private TableId table;

    private static TableRegistration reg() {
        return new TableRegistration(
                42L, "public", "events",
                List.of("id"), "event_time",
                "{\"unit\":\"hour\"}", "iceberg", "warehouse.public.events", null);
    }

    private record TestBatch(TableId table, int size, List<DeltaBatch.Key> keys)
            implements DeltaBatch {}

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        table = catalog.register(reg());
        catalog.initCutline(table, new TierKey(1000), new LakeSnapshotId(1));
    }

    @Test
    void registersAndLooksUpByName() {
        var found = catalog.lookup("public", "events");
        assertTrue(found.isPresent());
        assertEquals(table, found.get().id());
        assertEquals("iceberg", found.get().lakeFormat());
    }

    @Test
    void rejectsDuplicateRegistration() {
        assertThrows(CatalogException.class, () -> catalog.register(reg()));
    }

    @Test
    void advancesCutlineTogetherAndMonotonically() {
        catalog.advanceCutline(table, new TierKey(2000), new LakeSnapshotId(5));
        assertEquals(new Cutline(new TierKey(2000), new LakeSnapshotId(5)), catalog.readCutline(table));

        assertThrows(CatalogException.class,
                () -> catalog.advanceCutline(table, new TierKey(1500), new LakeSnapshotId(6)));
        assertThrows(CatalogException.class,
                () -> catalog.advanceCutline(table, new TierKey(2500), new LakeSnapshotId(4)));
    }

    @Test
    void mirrorFrontierSeedsThenAdvancesMonotonically() {
        assertTrue(catalog.readMirrorFrontier(table).isEmpty());

        catalog.advanceMirrorFrontier(table, new Lsn(100), new LakeSnapshotId(2), Map.of());
        assertEquals(new Lsn(100), catalog.readMirrorFrontier(table).orElseThrow());
        assertEquals(new LakeSnapshotId(2), catalog.readCutline(table).snapshot());

        assertThrows(CatalogException.class,
                () -> catalog.advanceMirrorFrontier(table, new Lsn(50), new LakeSnapshotId(3), Map.of()));
        assertThrows(CatalogException.class,
                () -> catalog.advanceMirrorFrontier(table, new Lsn(200), new LakeSnapshotId(1), Map.of()));
        assertEquals(new Lsn(100), catalog.readMirrorFrontier(table).orElseThrow());
    }

    @Test
    void compactionAdvancesSnapshotButLeavesTUnchanged() {
        catalog.publishCompaction(table, new LakeSnapshotId(9),
                new TestBatch(table, 1, List.of(new DeltaBatch.Key("3", 7))), Map.of());
        assertEquals(List.of(new DeltaBatch.Key("3", 7)), catalog.clearedDeltaKeys());
        Cutline c = catalog.readCutline(table);
        assertEquals(new TierKey(1000), c.t());
        assertEquals(new LakeSnapshotId(9), c.snapshot());
    }

    @Test
    void horizonIsOldestPinElseCutline() {
        assertEquals(new Cutline(new TierKey(1000), new LakeSnapshotId(1)), catalog.readHorizon(table));

        catalog.advanceCutline(table, new TierKey(2000), new LakeSnapshotId(10));
        long oldPin = catalog.addReadPin(table, new Cutline(new TierKey(1200), new LakeSnapshotId(4)));
        catalog.addReadPin(table, new Cutline(new TierKey(1600), new LakeSnapshotId(7)));
        assertEquals(new Cutline(new TierKey(1200), new LakeSnapshotId(4)), catalog.readHorizon(table));

        catalog.releaseReadPin(oldPin);
        assertEquals(new Cutline(new TierKey(1600), new LakeSnapshotId(7)), catalog.readHorizon(table));
    }

    @Test
    void loadLabelsDedupListStagedAndFlipOnFinish() {
        assertTrue(catalog.beginLoad(table, "b1", LoadState.STAGED,
                "[\"/wh/s/a.parquet\"]", "{\"rows\":10}"));
        assertTrue(catalog.beginLoad(table, "b2", LoadState.COMMITTED, null, "{\"rows\":1}"));
        assertEquals(false, catalog.beginLoad(table, "b1", LoadState.COMMITTED, null, null),
                "a replayed label never re-applies");

        assertEquals(List.of("b1"),
                catalog.stagedLoads(table).stream().map(LoadLabel::label).toList());

        catalog.finishLoad(table, List.of("b1"), new LakeSnapshotId(7), Map.of());
        assertTrue(catalog.stagedLoads(table).isEmpty());
        assertEquals(LoadState.COMMITTED, catalog.lookupLoad(table, "b1").orElseThrow().state());
        assertEquals(new LakeSnapshotId(7), catalog.readCutline(table).snapshot());
        assertEquals(new TierKey(1000), catalog.readCutline(table).t());

        // A concurrent commit already moved S past ours: no regression.
        catalog.finishLoad(table, List.of(), new LakeSnapshotId(3), Map.of());
        assertEquals(new LakeSnapshotId(7), catalog.readCutline(table).snapshot());
    }

    @Test
    void partitionFollowsLegalLifecycleOnly() {
        var p = new PartitionId(table, "2026-07-01T00");
        catalog.upsertPartition(p, new PartitionBounds(new TierKey(0), new TierKey(1000)), PartitionState.HOT);

        catalog.transition(p, PartitionState.HOT, PartitionState.SEALING);
        catalog.transition(p, PartitionState.SEALING, PartitionState.TIERING);

        assertThrows(IllegalTransitionException.class,
                () -> catalog.transition(p, PartitionState.TIERING, PartitionState.DROPPED));
        assertThrows(CatalogException.class,
                () -> catalog.transition(p, PartitionState.HOT, PartitionState.SEALING));

        catalog.transition(p, PartitionState.TIERING, PartitionState.TIERED);
        catalog.transition(p, PartitionState.TIERED, PartitionState.DROPPED);

        List<PartitionInfo> parts = catalog.listPartitions(table);
        assertEquals(1, parts.size());
        assertEquals(PartitionState.DROPPED, parts.get(0).state());
    }
}
