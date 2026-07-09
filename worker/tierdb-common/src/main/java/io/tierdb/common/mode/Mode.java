package io.tierdb.common.mode;

import java.util.Optional;

/**
 * Twin of {@code tierdb_core::mode::Mode}.
 *
 * <pre>
 * | mode                         | cold read              | hot write | cold write        |
 * |------------------------------|------------------------|-----------|-------------------|
 * | Tiered                       | pinned lake ⊕ delta    | heap      | delta buffer      |
 * | Tiered { keepHeap }          | pinned lake ⊕ delta    | heap      | heap + delta      |
 * | Direct                       | live lake, no overlay  | heap      | lake, synchronous |
 * | Direct { keepHeap }          | live lake, no overlay  | heap      | heap + lake       |
 * | Mirrored                     | (heap holds every row) | heap      | heap              |
 * | Mirrored { heapRetention }   | pinned lake ⊕ delta    | heap      | delta buffer      |
 * </pre>
 *
 * Deletes are the dual of inserts; updates compose the two.
 */
public sealed interface Mode {

    static Mode fromCatalog(String mode, boolean keepHeap, Long heapRetentionLag) {
        return switch (mode) {
            case "tiered" -> new Tiered(keepHeap);
            case "direct" -> new Direct(keepHeap);
            case "mirrored" -> new Mirrored(heapRetentionLag != null);
            default -> throw new IllegalArgumentException("unknown table mode '" + mode + "'");
        };
    }

    InsertPlan planInsert(RouteTarget target);

    boolean routesByCut();

    boolean heapComplete();

    boolean isDirect();

    default DeletePlan planDelete(RouteTarget target) {
        InsertPlan ins = planInsert(target);
        return new DeletePlan(
                ins.toHeap(),
                routesByCut() && target == RouteTarget.HOT,
                ins.cold(),
                routesByCut() && target == RouteTarget.COLD);
    }

    default UpdatePlan planUpdate(RouteTarget oldTarget, RouteTarget newTarget) {
        return new UpdatePlan(planDelete(oldTarget), planInsert(newTarget));
    }

    record Tiered(boolean keepHeap) implements Mode {
        @Override
        public InsertPlan planInsert(RouteTarget target) {
            boolean cold = target == RouteTarget.COLD;
            return new InsertPlan(
                    keepHeap || !cold,
                    cold ? Optional.of(ColdSink.DELTA) : Optional.empty(),
                    cold);
        }

        @Override
        public boolean routesByCut() {
            return !keepHeap;
        }

        @Override
        public boolean heapComplete() {
            return false;
        }

        @Override
        public boolean isDirect() {
            return false;
        }
    }

    record Direct(boolean keepHeap) implements Mode {
        @Override
        public InsertPlan planInsert(RouteTarget target) {
            boolean cold = target == RouteTarget.COLD;
            return new InsertPlan(
                    keepHeap || !cold,
                    cold ? Optional.of(ColdSink.LAKE) : Optional.empty(),
                    cold);
        }

        @Override
        public boolean routesByCut() {
            return !keepHeap;
        }

        @Override
        public boolean heapComplete() {
            return false;
        }

        @Override
        public boolean isDirect() {
            return true;
        }
    }

    record Mirrored(boolean heapRetention) implements Mode {
        @Override
        public InsertPlan planInsert(RouteTarget target) {
            boolean cold = heapRetention && target == RouteTarget.COLD;
            return new InsertPlan(
                    !cold, cold ? Optional.of(ColdSink.DELTA) : Optional.empty(), false);
        }

        @Override
        public boolean routesByCut() {
            return heapRetention;
        }

        @Override
        public boolean heapComplete() {
            return !heapRetention;
        }

        @Override
        public boolean isDirect() {
            return false;
        }
    }
}
