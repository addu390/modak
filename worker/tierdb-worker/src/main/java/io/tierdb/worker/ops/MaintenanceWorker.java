package io.tierdb.worker.ops;

import io.tierdb.catalog.Catalog;
import io.tierdb.catalog.LoadLabel;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.OpKind;
import io.tierdb.common.OpPhase;
import io.tierdb.lake.ColdTableSpec;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.commit.LakeTieringProps;
import io.tierdb.lake.maintain.MaintenanceEngine;
import io.tierdb.lake.maintain.MaintenancePlan;
import io.tierdb.lake.maintain.MaintenanceResult;
import io.tierdb.load.StagedFiles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;

/** The maintenance loop for one table: resolves the policy, computes the pinned floor, and hands the plan to the format's {@link MaintenanceEngine}. */
public final class MaintenanceWorker {

    private final Catalog catalog;
    private final LakeStorage lake;
    private final MaintenanceEngine engine;
    private final Map<String, String> defaultSettings;

    public MaintenanceWorker(Catalog catalog, LakeStorage lake, MaintenanceEngine engine,
            Map<String, String> defaultSettings) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
        this.engine = Objects.requireNonNull(engine);
        this.defaultSettings = Map.copyOf(defaultSettings);
    }

    public MaintenanceResult runCycle(RegisteredTable table, boolean force) {
        MaintenancePlan plan = buildPlan(table);
        if (!force && "false".equals(plan.settings().get("maintenance_enabled"))) {
            return MaintenanceResult.NOOP;
        }
        UUID opId = UUID.randomUUID();
        MaintenanceResult result = engine.run(
                lake.table(new CommitterInitContext(table.id(), table.lakeTableRef()),
                        new ColdTableSpec(table.primaryKeyCols(), table.tierKeyCol())),
                plan,
                LakeTieringProps.snapshotProps(opId, OpKind.MAINTENANCE, table.id()));
        if (force || !result.isNoop()) {
            catalog.logOpPhase(opId, table.id(), OpKind.MAINTENANCE,
                    OpPhase.ADVANCED, null, countersJson(result));
        }
        return result;
    }

    public MaintenancePlan buildPlan(RegisteredTable table) {
        return new MaintenancePlan(
                table.maintenancePolicy().resolve(defaultSettings),
                catalog.readHorizon(table.id()).snapshot().id(),
                stagedFilePaths(table));
    }

    private List<String> stagedFilePaths(RegisteredTable table) {
        List<String> paths = new ArrayList<>();
        for (LoadLabel label : catalog.stagedLoads(table.id())) {
            StagedFiles.fromJson(label.stagedFilesJson())
                    .ifPresent(staged -> paths.addAll(staged.files()));
        }
        return paths;
    }

    private static String countersJson(MaintenanceResult result) {
        StringJoiner json = new StringJoiner(",", "{", "}");
        result.counters().forEach((key, value) -> json.add("\"" + key + "\":" + value));
        return json.toString();
    }
}
