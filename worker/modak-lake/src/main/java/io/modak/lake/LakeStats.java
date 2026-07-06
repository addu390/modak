package io.modak.lake;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A point-in-time health snapshot of one cold table, produced by the format
 * plugin. Keys and meanings are format-owned, as are the health warnings.
 * The named constants are optional conventions for headline numbers.
 */
public record LakeStats(Map<String, Double> values, List<String> warnings) {

    public static final String FILES = "files";
    public static final String BYTES = "bytes";
    public static final String SNAPSHOTS = "snapshots";

    public static final LakeStats EMPTY = new LakeStats(Map.of(), List.of());

    public LakeStats {
        values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
        warnings = List.copyOf(warnings);
    }
}
