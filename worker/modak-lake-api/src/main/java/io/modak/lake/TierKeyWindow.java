package io.modak.lake;

/**
 * A half-open tier-key range {@code [minInclusive, maxExclusive)}. The cold
 * ingest window is the canonical instance: retention line up to cut-line.
 */
public record TierKeyWindow(long minInclusive, long maxExclusive) {

    public boolean contains(long key) {
        return key >= minInclusive && key < maxExclusive;
    }

    /** True when the closed range {@code [lo, hi]} lies entirely inside the window. */
    public boolean containsRange(long lo, long hi) {
        return lo >= minInclusive && hi < maxExclusive;
    }

    @Override
    public String toString() {
        return "[" + minInclusive + ", " + maxExclusive + ")";
    }
}
