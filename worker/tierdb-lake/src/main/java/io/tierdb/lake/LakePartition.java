package io.tierdb.lake;

import java.util.Locale;
import java.util.Set;

/**
 * Cold-table partition layout requested at registration. Truncate carries a
 * width in the tier key's native units, temporal transforms carry none.
 */
public record LakePartition(String transform, long truncateWidth) {

    public static final Set<String> TEMPORAL = Set.of("hour", "day", "month", "year");

    public static LakePartition none() {
        return new LakePartition("none", 0);
    }

    public static LakePartition truncate(long width) {
        return width > 0 ? new LakePartition("truncate", width) : none();
    }

    public static LakePartition temporal(String transform) {
        String t = transform.toLowerCase(Locale.ROOT);
        if (!TEMPORAL.contains(t)) {
            throw new IllegalArgumentException("unsupported lake partition transform: "
                    + transform + " (supported: hour, day, month, year, none)");
        }
        return new LakePartition(t, 0);
    }

    public boolean isNone() {
        return transform.equals("none");
    }

    public boolean isTruncate() {
        return transform.equals("truncate");
    }
}
