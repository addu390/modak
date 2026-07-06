package io.tierdb.lake.maintain;

import java.util.List;
import java.util.Map;

/** One table's maintenance order, with format-interpreted {@code settings}. */
public record MaintenancePlan(
        Map<String, String> settings,
        long pinnedSnapshotFloor,
        List<String> protectedFiles) {

    public MaintenancePlan {
        settings = Map.copyOf(settings);
        protectedFiles = List.copyOf(protectedFiles);
    }
}
