package io.tierdb.lake.access;

import java.util.List;

public sealed interface ColumnConstraint {

    record None() implements ColumnConstraint {}

    record OnlyNull() implements ColumnConstraint {}

    record Ranges(List<ValueRange> ranges, boolean nullAllowed) implements ColumnConstraint {}

    record ValueRange(Object low, boolean lowInclusive, Object high, boolean highInclusive) {

        public boolean isSingleValue() {
            return low != null && low.equals(high) && lowInclusive && highInclusive;
        }
    }
}
