package io.tierdb.common;

/**
 * Partition lifecycle, in order HOT, SEALING, TIERING, TIERED, DROPPED.
 * Reclaim (TIERED to DROPPED) is a partition DROP/DETACH (DDL), never a
 * row-level DELETE, so aging-out stays invisible to any CDC mirror (invariant #7).
 */
public enum PartitionState {
    HOT,
    SEALING,
    TIERING,
    TIERED,
    DROPPED;

    public boolean canTransitionTo(PartitionState next) {
        return switch (this) {
            case HOT -> next == SEALING;
            case SEALING -> next == TIERING;
            case TIERING -> next == TIERED;
            case TIERED -> next == DROPPED;
            case DROPPED -> false;
        };
    }
}
