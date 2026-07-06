package io.tierdb.common;

/**
 * A pinned version of the cold store, format-agnostic. Modeled as a monotonic
 * {@code long}, which fits every long-versioned lake format (Iceberg, Paimon,
 * Delta). This is the single place that assumes a long version.
 */
public record LakeSnapshotId(long id) implements Comparable<LakeSnapshotId> {
    @Override
    public int compareTo(LakeSnapshotId o) {
        return Long.compare(this.id, o.id);
    }
}
