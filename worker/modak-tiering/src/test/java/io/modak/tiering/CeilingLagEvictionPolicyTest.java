package io.modak.tiering;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.catalog.PartitionInfo;
import io.modak.common.Cutline;
import io.modak.common.LakeSnapshotId;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import org.junit.jupiter.api.Test;

/** The two-clock retention rule: DROP measures from the partition ceiling. */
class CeilingLagEvictionPolicyTest {

    private static final Cutline HORIZON =
            new Cutline(new TierKey(1000), new LakeSnapshotId(9));

    private static PartitionInfo tiered(long lo, long hi) {
        return partition(lo, hi, PartitionState.TIERED);
    }

    private static PartitionInfo partition(long lo, long hi, PartitionState state) {
        return new PartitionInfo(new PartitionId(new TableId(1L), "p_" + lo),
                new PartitionBounds(new TierKey(lo), new TierKey(hi)), state);
    }

    @Test
    void reclaimsOnlyWhenTheCeilingIsBehindTheFrontierByTheLag() {
        var policy = new CeilingLagEvictionPolicy(() -> 1000L, 500);

        assertTrue(policy.canReclaim(tiered(0, 500), HORIZON), "ceiling 500 <= 1000 - 500");
        assertFalse(policy.canReclaim(tiered(500, 600), HORIZON),
                "ceiling 600 is too close to the frontier for a destructive DROP");
    }

    @Test
    void sealGateStillApplies() {
        var policy = new CeilingLagEvictionPolicy(() -> 10_000L, 0);

        assertFalse(policy.canReclaim(partition(0, 100, PartitionState.HOT), HORIZON),
                "untired partitions are never reclaimed regardless of age");
        assertFalse(policy.canReclaim(tiered(900, 1100), HORIZON),
                "a pinned reader below the ceiling blocks the DROP");
    }

    @Test
    void emptyHotTableReclaimsNothing() {
        var policy = new CeilingLagEvictionPolicy(() -> null, 0);
        assertFalse(policy.canReclaim(tiered(0, 100), HORIZON),
                "no frontier means nothing is aging, stay conservative");
    }

    @Test
    void frontierIsProbedOnce() {
        int[] probes = {0};
        var policy = new CeilingLagEvictionPolicy(() -> {
            probes[0]++;
            return 1000L;
        }, 0);
        policy.canReclaim(tiered(0, 100), HORIZON);
        policy.canReclaim(tiered(100, 200), HORIZON);
        assertTrue(probes[0] == 1, "one frontier probe per cycle, not per partition");
    }
}
