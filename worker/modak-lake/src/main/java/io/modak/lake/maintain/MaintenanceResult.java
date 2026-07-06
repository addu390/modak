package io.modak.lake.maintain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What one maintenance pass did, as format-owned counters (operation name to
 * count). All-zero or empty means the pass was a no-op.
 */
public record MaintenanceResult(Map<String, Long> counters) {

    public static final MaintenanceResult NOOP = new MaintenanceResult(Map.of());

    public MaintenanceResult {
        counters = Collections.unmodifiableMap(new LinkedHashMap<>(counters));
    }

    public boolean isNoop() {
        return counters.values().stream().allMatch(v -> v == 0);
    }

    public long counter(String key) {
        return counters.getOrDefault(key, 0L);
    }
}
