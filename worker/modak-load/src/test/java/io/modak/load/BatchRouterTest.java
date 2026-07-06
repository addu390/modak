package io.modak.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.connector.seam.SeamState;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BatchRouterTest {

    private static final List<String> PK = List.of("id");
    private static final long T = 100;

    private static SeamState seam(Long retentionLine, boolean heapOnly) {
        return new SeamState(42L, PK, "ts", "bigint", heapOnly ? "mirrored" : "tiered",
                "iceberg", "ref", null, null, null, T, retentionLine, null, null);
    }

    private static Map<String, Object> row(long id, long ts) {
        return Map.of("id", id, "ts", ts);
    }

    @Test
    void splitsAtTheCutlineInclusively() {
        var routed = BatchRouter.route(
                List.of(row(1, 99), row(2, 100), row(3, 101)),
                seam(null, false), true, 1000);

        assertEquals(2, routed.hot().size(), "tier_key >= T is hot");
        assertEquals(1, routed.coldDelta().size());
        assertEquals(99L, routed.coldDelta().get(0).get("ts"));
        assertTrue(routed.coldSpool().isEmpty());
    }

    @Test
    void coldVolumeAboveTheThresholdSpools() {
        var routed = BatchRouter.route(
                List.of(row(1, 10), row(2, 20), row(3, 30), row(4, 200)),
                seam(null, false), true, 2);

        assertEquals(1, routed.hot().size());
        assertTrue(routed.coldDelta().isEmpty(), "over the threshold nothing trickles to delta");
        assertEquals(3, routed.coldSpool().size());
    }

    @Test
    void coldVolumeStaysInDeltaWhenNoLakeStorageIsAvailable() {
        var routed = BatchRouter.route(
                List.of(row(1, 10), row(2, 20), row(3, 30)),
                seam(null, false), false, 1);

        assertEquals(3, routed.coldDelta().size());
        assertTrue(routed.coldSpool().isEmpty());
    }

    @Test
    void aRowBelowTheRetentionLineRejectsTheBatch() {
        LoadException e = assertThrows(LoadException.class, () -> BatchRouter.route(
                List.of(row(1, 150), row(2, 49)), seam(50L, false), true, 1000));
        assertTrue(e.getMessage().contains("retention line"), e.getMessage());

        var routed = BatchRouter.route(List.of(row(2, 50)), seam(50L, false), true, 1000);
        assertEquals(1, routed.coldDelta().size());
    }

    @Test
    void duplicatePksWithinOneBatchReject() {
        LoadException e = assertThrows(LoadException.class, () -> BatchRouter.route(
                List.of(row(7, 150), row(7, 160)), seam(null, false), true, 1000));
        assertTrue(e.getMessage().contains("repeats a primary key"), e.getMessage());
    }

    @Test
    void fullyMirroredTablesRouteEverythingToTheHeap() {
        var routed = BatchRouter.route(
                List.of(row(1, 5), row(2, 500)), seam(null, true), true, 0);

        assertEquals(2, routed.hot().size());
        assertTrue(routed.coldDelta().isEmpty());
        assertTrue(routed.coldSpool().isEmpty());
    }

    @Test
    void missingTierKeyOrPkRejects() {
        assertThrows(LoadException.class, () -> BatchRouter.route(
                List.of(Map.of("id", 1L)), seam(null, false), true, 1000));
        assertThrows(LoadException.class, () -> BatchRouter.route(
                List.of(Map.of("ts", 150L)), seam(null, false), true, 1000));
    }

    @Test
    void compositePksAreEncodedCanonically() {
        var rows = List.<Map<String, Object>>of(Map.of("a", "x", "b", "y", "ts", 150L));
        assertEquals("x\u001fy", BatchRouter.encodePk(rows.get(0), List.of("a", "b"), 1));
    }
}
