package io.tierdb.lake;

import java.util.List;
import java.util.Objects;

/**
 * The registered-table facts a cold-store format needs for pk-aware and
 * tier-aware operations, so they are stated once per table handle instead of
 * on every call.
 */
public record ColdTableSpec(List<String> pkCols, String tierKeyCol) {

    public ColdTableSpec {
        pkCols = List.copyOf(pkCols);
        Objects.requireNonNull(tierKeyCol, "tierKeyCol");
    }
}
