package io.tierdb.common.mode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The Java half of the mode conformance suite; mirrors the matrix tests in
 * {@code tierdb-core/src/mode.rs} case-for-case.
 */
class ModeTest {

    private static final List<Mode> ALL_MODES = List.of(
            new Mode.Tiered(false),
            new Mode.Tiered(true),
            new Mode.Direct(false),
            new Mode.Direct(true),
            new Mode.Mirrored(false),
            new Mode.Mirrored(true));

    private record InsertCase(Mode mode, RouteTarget target, InsertPlan want) {}

    @Test
    void theInsertRoutingMatrix() {
        List<InsertCase> matrix = List.of(
                new InsertCase(new Mode.Tiered(false), RouteTarget.HOT,
                        plan(true, null, false)),
                new InsertCase(new Mode.Tiered(false), RouteTarget.COLD,
                        plan(false, ColdSink.DELTA, true)),
                new InsertCase(new Mode.Tiered(true), RouteTarget.HOT,
                        plan(true, null, false)),
                new InsertCase(new Mode.Tiered(true), RouteTarget.COLD,
                        plan(true, ColdSink.DELTA, true)),
                new InsertCase(new Mode.Direct(false), RouteTarget.HOT,
                        plan(true, null, false)),
                new InsertCase(new Mode.Direct(false), RouteTarget.COLD,
                        plan(false, ColdSink.LAKE, true)),
                new InsertCase(new Mode.Direct(true), RouteTarget.HOT,
                        plan(true, null, false)),
                new InsertCase(new Mode.Direct(true), RouteTarget.COLD,
                        plan(true, ColdSink.LAKE, true)),
                new InsertCase(new Mode.Mirrored(false), RouteTarget.HOT,
                        plan(true, null, false)),
                new InsertCase(new Mode.Mirrored(false), RouteTarget.COLD,
                        plan(true, null, false)),
                new InsertCase(new Mode.Mirrored(true), RouteTarget.HOT,
                        plan(true, null, false)),
                new InsertCase(new Mode.Mirrored(true), RouteTarget.COLD,
                        plan(false, ColdSink.DELTA, false)));

        assertEquals(ALL_MODES.size() * 2, matrix.size(), "matrix covers every case");
        for (InsertCase c : matrix) {
            assertEquals(c.want(), c.mode().planInsert(c.target()),
                    c.mode() + " " + c.target());
        }
    }

    @Test
    void heapCompleteIsOnlyTrueForFullHeapMirrored() {
        assertFalse(new Mode.Tiered(false).heapComplete());
        assertFalse(new Mode.Tiered(true).heapComplete());
        assertFalse(new Mode.Direct(false).heapComplete());
        assertFalse(new Mode.Direct(true).heapComplete());
        assertTrue(new Mode.Mirrored(false).heapComplete(),
                "mirrored without retention holds every row on the heap");
        assertFalse(new Mode.Mirrored(true).heapComplete());
    }

    @Test
    void isDirectIsOnlyTrueForDirectVariants() {
        assertFalse(new Mode.Tiered(false).isDirect());
        assertFalse(new Mode.Tiered(true).isDirect());
        assertTrue(new Mode.Direct(false).isDirect());
        assertTrue(new Mode.Direct(true).isDirect());
        assertFalse(new Mode.Mirrored(false).isDirect());
        assertFalse(new Mode.Mirrored(true).isDirect());
    }

    @Test
    void deleteIsTheDualOfInsertForEveryMode() {
        for (Mode mode : ALL_MODES) {
            for (RouteTarget target : RouteTarget.values()) {
                InsertPlan ins = mode.planInsert(target);
                DeletePlan del = mode.planDelete(target);
                assertEquals(ins.toHeap(), del.fromHeap(), mode + " " + target);
                assertEquals(ins.cold(), del.cold(), mode + " " + target);
                assertEquals(mode.routesByCut() && target == RouteTarget.HOT,
                        del.heapFloor(), mode + " " + target);
                assertEquals(mode.routesByCut() && target == RouteTarget.COLD,
                        del.checkRetention(), mode + " " + target);
            }
        }
    }

    @Test
    void updateComposesADeleteAndAnInsert() {
        for (Mode mode : ALL_MODES) {
            for (RouteTarget old : RouteTarget.values()) {
                for (RouteTarget now : RouteTarget.values()) {
                    UpdatePlan up = mode.planUpdate(old, now);
                    assertEquals(mode.planDelete(old), up.removeOld(), mode.toString());
                    assertEquals(mode.planInsert(now), up.writeNew(), mode.toString());
                }
            }
        }
    }

    @Test
    void catalogRowsMapToModes() {
        assertEquals(new Mode.Tiered(false), Mode.fromCatalog("tiered", false, null));
        assertEquals(new Mode.Tiered(true), Mode.fromCatalog("tiered", true, null));
        assertEquals(new Mode.Direct(true), Mode.fromCatalog("direct", true, null));
        assertEquals(new Mode.Mirrored(false), Mode.fromCatalog("mirrored", false, null));
        assertEquals(new Mode.Mirrored(true), Mode.fromCatalog("mirrored", false, 3_600L));
        assertThrows(IllegalArgumentException.class,
                () -> Mode.fromCatalog("archived", false, null));
    }

    private static InsertPlan plan(boolean toHeap, ColdSink cold, boolean checkRetention) {
        return new InsertPlan(toHeap, Optional.ofNullable(cold), checkRetention);
    }
}
