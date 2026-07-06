package io.tierdb.catalog;

import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;

/** A row of {@code tierdb.partitions} as read back. */
public record PartitionInfo(PartitionId id, PartitionBounds bounds, PartitionState state) {}
