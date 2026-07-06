package io.tierdb.common;

/** Half-open tier-key bounds {@code [lo, hi)} of a partition. */
public record PartitionBounds(TierKey lo, TierKey hi) {}
