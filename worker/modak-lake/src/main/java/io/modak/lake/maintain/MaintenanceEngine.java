package io.modak.lake.maintain;

import io.modak.lake.LakeTable;
import java.util.Map;

/** Executes a {@link MaintenancePlan} against one cold table. */
public interface MaintenanceEngine {

    MaintenanceResult run(LakeTable table, MaintenancePlan plan,
            Map<String, String> snapshotProps);
}
