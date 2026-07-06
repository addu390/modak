package io.tierdb.worker.ops;

import io.tierdb.lake.LakeTable;
import io.tierdb.lake.maintain.MaintenanceEngine;
import io.tierdb.lake.maintain.MaintenancePlan;
import io.tierdb.lake.maintain.MaintenanceResult;
import java.util.Map;

/** The in-worker engine: executes the plan in process via the format plugin. */
public final class EmbeddedMaintenanceEngine implements MaintenanceEngine {

    @Override
    public MaintenanceResult run(LakeTable table, MaintenancePlan plan,
            Map<String, String> snapshotProps) {
        return table.maintain(plan, snapshotProps);
    }
}
