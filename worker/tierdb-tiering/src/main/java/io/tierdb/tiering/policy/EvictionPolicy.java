package io.tierdb.tiering.policy;

import io.tierdb.catalog.PartitionInfo;
import io.tierdb.common.Cutline;

/**
 * The CDC-readiness seam: decides whether a tiered partition may be physically
 * reclaimed. {@code horizon} is the oldest pinned {@code (T, S)} across active
 * readers. Reclaim is always a partition DROP/DETACH (DDL), never a row DELETE.
 */
public interface EvictionPolicy {
    boolean canReclaim(PartitionInfo partition, Cutline horizon);
}
