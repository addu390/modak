package io.tierdb.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SeamTypesTest {

    @Test
    void tierKeyOrdersByValue() {
        assertTrue(new TierKey(100).compareTo(new TierKey(101)) < 0);
        assertTrue(new TierKey(100).compareTo(new TierKey(99)) > 0);
        assertEquals(0, new TierKey(100).compareTo(new TierKey(100)));
    }

    @Test
    void lakeSnapshotIdIsMonotonicComparable() {
        assertTrue(new LakeSnapshotId(6).compareTo(new LakeSnapshotId(7)) < 0);
        assertEquals(0, new LakeSnapshotId(7).compareTo(new LakeSnapshotId(7)));
    }
}
