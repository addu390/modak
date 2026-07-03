package io.modak.common;

/**
 * An opaque stream of rows read from a hot Postgres partition, to be written to the
 * lake by a {@code LakeWriter}. The concrete row representation is defined by the
 * source/lake adapters, the core treats it as an opaque carrier.
 */
public interface PartitionData {
    PartitionId partitionId();
    PartitionBounds bounds();

    /** Row count when the carrier knows it, -1 when opaque. */
    default long rowCount() {
        return -1;
    }
}
