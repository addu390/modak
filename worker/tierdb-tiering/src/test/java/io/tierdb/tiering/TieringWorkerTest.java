package io.tierdb.tiering;

import io.tierdb.common.OpKind;
import io.tierdb.tiering.policy.SealGatedEvictionPolicy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.InMemoryCatalog;
import io.tierdb.catalog.TableMode;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.Cutline;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import java.time.Instant;
import java.util.List;

import io.tierdb.lake.commit.LakeTieringProps;
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
                "{\"unit\":\"hour\"}", "fake", "/fake/events"));
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
        return stateOf(table, id);
    }

    private PartitionState stateOf(TableId owner, PartitionId id) {
        return catalog.listPartitions(owner).stream()
                .filter(p -> p.id().equals(id)).findFirst().orElseThrow().state();
    }

    @Test
    void oneCycleTiersCommitsAdvancesAndReclaims() {
        selected = List.of(p0, p1);
        worker.runCycle(table, NOW);

        assertEquals(1, lake.snapshots.size());
        var snap = lake.snapshots.get(0);
        assertEquals("200", snap.props().get(LakeTieringProps.NEW_TIER_KEY_HI));
        assertEquals(OpKind.TIERING.commitUser(),
                snap.props().get(LakeTieringProps.COMMIT_USER),
                "ecosystem-standard commit-user stamp");
        assertEquals(3, lake.allRows().size(), "both partitions' rows in the one commit");

        assertEquals(new Cutline(new TierKey(200), new LakeSnapshotId(1)), catalog.readCutline(table));
        assertTrue(catalog.readLakeProps(table).orElseThrow().contains("metadata_location"));

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
        assertTrue(catalog.readLakeProps(table).orElseThrow().contains("metadata_location"));
        assertTrue(catalog.findIncompleteOps(table, OpKind.TIERING).isEmpty());
        assertEquals(PartitionState.DROPPED, stateOf(p0), "and reclaim proceeds");
    }

    @Test
    void catalogBehindTheLakeIsBackfilledAndTheCommitAborted() {
        lake.seedSnapshot(java.util.Map.of(
                LakeTieringProps.OP_ID, java.util.UUID.randomUUID().toString(),
                LakeTieringProps.OP_KIND, OpKind.TIERING.sql(),
                LakeTieringProps.NEW_TIER_KEY_HI, "100",
                LakeTieringProps.TABLE_ID, "42"),
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
        lake.seedSnapshot(java.util.Map.of(
                LakeTieringProps.OP_ID, java.util.UUID.randomUUID().toString(),
                LakeTieringProps.OP_KIND, OpKind.TIERING.sql(),
                LakeTieringProps.NEW_TIER_KEY_HI, "100",
                LakeTieringProps.TABLE_ID, "999"),
                List.<Object[]>of(new Object[] {99L, 10L, "stale"}));

        selected = List.of(p0);
        worker.runCycle(table, NOW);

        assertEquals(0, lake.abortedCommittables, "foreign snapshot is ignored, not a conflict");
        assertEquals(2, lake.snapshots.size(), "our own commit lands on top");
        assertEquals(new Cutline(new TierKey(100), new LakeSnapshotId(2)), catalog.readCutline(table));
        assertEquals("42", lake.snapshots.get(1).props().get(LakeTieringProps.TABLE_ID),
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
    void keepHeapTiersAndAdvancesButNeverDropsPartitions() {
        TableId kh = catalog.register(new TableRegistration(
                43L, "public", "readings", List.of("id"), "event_time",
                "{\"unit\":\"hour\"}", "fake", "/fake/readings",
                TableMode.TIERED, null, null,
                java.util.Optional.empty(), java.util.Optional.empty(), true));
        catalog.initCutline(kh, new TierKey(0), new LakeSnapshotId(0));
        PartitionId k0 = new PartitionId(kh, "readings_p0");
        catalog.upsertPartition(k0, new PartitionBounds(new TierKey(0), new TierKey(100)),
                PartitionState.HOT);
        hotSource.seed(k0, new Object[] {1L, 10L, "a"});

        selected = List.of(k0);
        worker.runCycle(kh, NOW);

        assertEquals(1, lake.snapshots.size());
        assertEquals(new Cutline(new TierKey(100), new LakeSnapshotId(1)), catalog.readCutline(kh));
        assertEquals(List.of(k0), hotSource.mirrored, "cold mirror attached before the copy");
        assertTrue(hotSource.dropped.isEmpty(), "the heap keeps its copy");
        assertEquals(PartitionState.TIERED, stateOf(kh, k0));

        selected = List.of();
        worker.runCycle(kh, NOW);
        assertTrue(hotSource.dropped.isEmpty(), "later cycles never reclaim either");
        assertEquals(PartitionState.TIERED, stateOf(kh, k0));
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
