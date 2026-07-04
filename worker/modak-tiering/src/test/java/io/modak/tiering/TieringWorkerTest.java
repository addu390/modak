package io.modak.tiering;

import io.modak.common.OpKind;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.catalog.InMemoryCatalog;
import io.modak.catalog.PartitionInfo;
import io.modak.catalog.TableRegistration;
import io.modak.common.Cutline;
import io.modak.common.LakeSnapshotId;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The tiering protocol on fakes: one lake commit per cycle, atomic advance, crash
 * recovery on both sides of the commit point, and horizon-gated reclaim.
 */
class TieringWorkerTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    private InMemoryCatalog catalog;
    private FakeLake lake;
    private FakeHotSource hotSource;
    private TableId table;
    private PartitionId p0;
    private PartitionId p1;
    private PartitionId pHot;

    private List<PartitionId> selected = List.of();

    private TieringWorker worker;

    @BeforeEach
    void setUp() {
        catalog = new InMemoryCatalog();
        lake = new FakeLake();
        hotSource = new FakeHotSource();
        table = catalog.register(new TableRegistration(
                42L, "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"hour\"}", "fake", "/fake/events", null));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        p0 = partition("events_p0", 0, 100);
        p1 = partition("events_p1", 100, 200);
        pHot = partition("events_p2", 200, 300);
        hotSource.seed(p0, new Object[] {1L, 10L, "a"}, new Object[] {2L, 20L, "b"});
        hotSource.seed(p1, new Object[] {3L, 110L, "c"});

        worker = new TieringWorker(catalog, lake, hotSource,
                (t, now) -> selected, new SealGatedEvictionPolicy());
    }

    private PartitionId partition(String name, long lo, long hi) {
        PartitionId id = new PartitionId(table, name);
        catalog.upsertPartition(id, new PartitionBounds(new TierKey(lo), new TierKey(hi)),
                PartitionState.HOT);
        return id;
    }

    private PartitionState stateOf(PartitionId id) {
        return catalog.listPartitions(table).stream()
                .filter(p -> p.id().equals(id)).findFirst().orElseThrow().state();
    }

    @Test
    void oneCycleTiersCommitsAdvancesAndReclaims() {
        selected = List.of(p0, p1);
        worker.runCycle(table, NOW);

        assertEquals(1, lake.snapshots.size());
        var snap = lake.snapshots.get(0);
        assertEquals("200", snap.props().get(io.modak.lake.LakeTieringProps.NEW_TIER_KEY_HI));
        assertEquals(io.modak.common.OpKind.TIERING.commitUser(),
                snap.props().get(io.modak.lake.LakeTieringProps.COMMIT_USER),
                "ecosystem-standard commit-user stamp");
        assertEquals(3, lake.allRows().size(), "both partitions' rows in the one commit");

        assertEquals(new Cutline(new TierKey(200), new LakeSnapshotId(1)), catalog.readCutline(table));
        assertTrue(catalog.get(table).orElseThrow().lakeProps().contains("metadata_location"));

        assertEquals(List.of(p0, p1), hotSource.dropped);
        assertEquals(PartitionState.DROPPED, stateOf(p0));
        assertEquals(PartitionState.DROPPED, stateOf(p1));
        assertEquals(PartitionState.HOT, stateOf(pHot), "unselected partition untouched");

        assertTrue(catalog.findIncompleteOps(table, OpKind.TIERING).isEmpty());
    }

    @Test
    void reclaimIsGatedByThePinnedReaderHorizon() {
        long pin = catalog.addReadPin(table, new Cutline(new TierKey(0), new LakeSnapshotId(0)));

        selected = List.of(p0, p1);
        worker.runCycle(table, NOW);

        assertEquals(new TierKey(200), catalog.readCutline(table).t(), "tiering itself proceeds");
        assertTrue(hotSource.dropped.isEmpty(), "no partition dropped under the pin");
        assertEquals(PartitionState.TIERED, stateOf(p0));

        catalog.releaseReadPin(pin);
        selected = List.of();
        worker.runCycle(table, NOW);
        assertEquals(List.of(p0, p1), hotSource.dropped);
        assertEquals(PartitionState.DROPPED, stateOf(p0));
    }

    @Test
    void crashBeforeLakeCommitAbandonsTheOpAndReTiersCleanly() {
        selected = List.of(p0);
        lake.failBeforeCommit = true;
        assertThrows(TieringException.class, () -> worker.runCycle(table, NOW));

        assertEquals(0, lake.snapshots.size());
        assertEquals(new TierKey(0), catalog.readCutline(table).t());
        assertEquals(1, catalog.findIncompleteOps(table, OpKind.TIERING).size());

        lake.failBeforeCommit = false;
        worker.runCycle(table, NOW);

        assertEquals(1, lake.snapshots.size(), "data committed exactly once");
        assertEquals(new Cutline(new TierKey(100), new LakeSnapshotId(1)), catalog.readCutline(table));
        assertTrue(catalog.findIncompleteOps(table, OpKind.TIERING).isEmpty());
        assertEquals(PartitionState.DROPPED, stateOf(p0));
    }

    @Test
    void crashAfterLakeCommitResumesByAdvancingNotRecommitting() {
        selected = List.of(p0);
        lake.failAfterCommit = true;
        assertThrows(TieringException.class, () -> worker.runCycle(table, NOW));

        assertEquals(1, lake.snapshots.size());
        assertEquals(new TierKey(0), catalog.readCutline(table).t());

        lake.failAfterCommit = false;
        int writersBefore = lake.writersCreated;
        selected = List.of();
        worker.runCycle(table, NOW);

        assertEquals(1, lake.snapshots.size(), "no second commit of the same data");
        assertEquals(writersBefore, lake.writersCreated, "no re-flush either");
        assertEquals(new Cutline(new TierKey(100), new LakeSnapshotId(1)), catalog.readCutline(table));
        assertTrue(catalog.get(table).orElseThrow().lakeProps().contains("metadata_location"));
        assertTrue(catalog.findIncompleteOps(table, OpKind.TIERING).isEmpty());
        assertEquals(PartitionState.DROPPED, stateOf(p0), "and reclaim proceeds");
    }

    @Test
    void catalogBehindTheLakeIsBackfilledAndTheCommitAborted() {
        // A tiering snapshot (p0, T=100) the catalog never learned about, unclaimed by any op.
        lake.seedSnapshot(java.util.Map.of(
                io.modak.lake.LakeTieringProps.OP_ID, java.util.UUID.randomUUID().toString(),
                io.modak.lake.LakeTieringProps.OP_KIND, io.modak.common.OpKind.TIERING.sql(),
                io.modak.lake.LakeTieringProps.NEW_TIER_KEY_HI, "100",
                io.modak.lake.LakeTieringProps.TABLE_ID, "42"),
                List.<Object[]>of(new Object[] {1L, 10L, "a"}, new Object[] {2L, 20L, "b"}));

        selected = List.of(p0, p1);
        assertThrows(TieringException.class, () -> worker.runCycle(table, NOW));

        assertEquals(1, lake.snapshots.size(), "no commit on top of the unknown snapshot");
        assertEquals(1, lake.abortedCommittables, "this op's files aborted");
        assertEquals(new Cutline(new TierKey(100), new LakeSnapshotId(1)), catalog.readCutline(table),
                "catalog backfilled from the snapshot's own stamps");
        assertEquals(PartitionState.TIERED, stateOf(p0));
        assertTrue(catalog.findIncompleteOps(table, OpKind.TIERING).isEmpty(),
                "the aborted op is closed in the log");

        selected = List.of(p1);
        worker.runCycle(table, NOW);
        assertEquals(2, lake.snapshots.size(), "next cycle re-tiers cleanly");
        assertEquals(new Cutline(new TierKey(200), new LakeSnapshotId(2)), catalog.readCutline(table));
        assertEquals(PartitionState.DROPPED, stateOf(p0));
        assertEquals(PartitionState.DROPPED, stateOf(p1));
    }

    @Test
    void foreignIncarnationSnapshotNeverAdvancesThisTable() {
        // Same location, different stamped table_id: a dropped + re-registered incarnation.
        lake.seedSnapshot(java.util.Map.of(
                io.modak.lake.LakeTieringProps.OP_ID, java.util.UUID.randomUUID().toString(),
                io.modak.lake.LakeTieringProps.OP_KIND, io.modak.common.OpKind.TIERING.sql(),
                io.modak.lake.LakeTieringProps.NEW_TIER_KEY_HI, "100",
                io.modak.lake.LakeTieringProps.TABLE_ID, "999"),
                List.<Object[]>of(new Object[] {99L, 10L, "stale"}));

        selected = List.of(p0);
        worker.runCycle(table, NOW);

        assertEquals(0, lake.abortedCommittables, "foreign snapshot is ignored, not a conflict");
        assertEquals(2, lake.snapshots.size(), "our own commit lands on top");
        assertEquals(new Cutline(new TierKey(100), new LakeSnapshotId(2)), catalog.readCutline(table));
        assertEquals("42", lake.snapshots.get(1).props().get(io.modak.lake.LakeTieringProps.TABLE_ID),
                "our commits carry the identity stamp");
        assertEquals(PartitionState.DROPPED, stateOf(p0));
    }

    @Test
    void nonContiguousBatchIsRejectedBeforeAnySideEffect() {
        selected = List.of(p1);
        assertThrows(TieringException.class, () -> worker.runCycle(table, NOW));
        assertEquals(0, lake.snapshots.size());
        assertEquals(new TierKey(0), catalog.readCutline(table).t());
        assertEquals(PartitionState.HOT, stateOf(p1));
    }

    @Test
    void emptyFlushSkipsTheLakeCommitButStillAdvancesTheCutline() {
        hotSource.rowsByPartition.clear();

        selected = List.of(p0, p1);
        worker.runCycle(table, NOW);

        assertEquals(0, lake.snapshots.size(), "no no-op snapshot in the lake history");
        assertEquals(new Cutline(new TierKey(200), new LakeSnapshotId(0)), catalog.readCutline(table),
                "T advances so the policy stops re-selecting; S is unchanged");
        assertEquals(PartitionState.DROPPED, stateOf(p0));
        assertEquals(PartitionState.DROPPED, stateOf(p1));
        assertTrue(catalog.findIncompleteOps(table, OpKind.TIERING).isEmpty());
    }

    @Test
    void reclaimFailureIsDistinctAndNeverUnwindsTheCommit() {
        selected = List.of(p0);
        hotSource.failOnDrop = true;
        assertThrows(ReclaimException.class, () -> worker.runCycle(table, NOW));

        assertEquals(1, lake.snapshots.size(), "the commit stands");
        assertEquals(new Cutline(new TierKey(100), new LakeSnapshotId(1)), catalog.readCutline(table),
                "the advance stands");
        assertEquals(PartitionState.TIERED, stateOf(p0), "only the DROP is outstanding");

        hotSource.failOnDrop = false;
        selected = List.of();
        worker.runCycle(table, NOW);
        assertEquals(PartitionState.DROPPED, stateOf(p0), "DROP retries and succeeds");
    }

    @Test
    void emptyPolicySelectionStillRunsReclaim() {
        selected = List.of(p0);
        long pin = catalog.addReadPin(table, new Cutline(new TierKey(0), new LakeSnapshotId(0)));
        worker.runCycle(table, NOW);
        catalog.releaseReadPin(pin);

        selected = List.of();
        worker.runCycle(table, NOW);
        assertEquals(List.of(p0), hotSource.dropped);
    }
}
