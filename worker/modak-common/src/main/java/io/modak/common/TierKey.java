package io.modak.common;

/**
 * A value along a table's tier-key (e.g. epoch micros). Defines data
 * temperature and aging order. A write that changes a row's tier-key moves
 * the row to the side of the cut-line the new value falls on.
 */
public record TierKey(long value) implements Comparable<TierKey> {
    @Override
    public int compareTo(TierKey o) {
        return Long.compare(this.value, o.value);
    }
}
