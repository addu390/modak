package io.tierdb.tiering.policy;

import io.tierdb.catalog.PartitionInfo;
import io.tierdb.common.Cutline;
import io.tierdb.common.PartitionState;

/**
 * Eviction gate, reclaimable once {@code TIERED} <b>and</b> the whole range
 * lies below the oldest pinned {@code T}. A reader pinned at T reads everything
 * below it from its pinned cold snapshot, which already holds this partition's rows.
 */
public final class SealGatedEvictionPolicy implements EvictionPolicy {
    @Override
    public boolean canReclaim(PartitionInfo partition, Cutline horizon) {
        return partition.state() == PartitionState.TIERED
                && partition.bounds().hi().compareTo(horizon.t()) <= 0;
    }
}
