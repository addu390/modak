package io.modak.worker;

import io.modak.catalog.Catalog;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TieringOp;
import io.modak.common.LakeSnapshotId;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringProps;
import io.modak.lake.MaintenanceConfig;
import io.modak.lake.MaintenanceResult;
import java.util.Objects;
import java.util.UUID;

/**
 * The lake maintenance loop for one table. Bin-packs small data files and
 * expires old snapshots, gated on the pinned reader horizon (nothing at or
 * above the oldest pinned S is ever expired). Both halves are single atomic
 * lake commits, so a crash mid-pass loses nothing and the next pass re-runs.
 */
final class MaintenanceWorker {

    private final Catalog catalog;
    private final LakeStorage lake;
    private final MaintenanceConfig config;

    MaintenanceWorker(Catalog catalog, LakeStorage lake, MaintenanceConfig config) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
        this.config = Objects.requireNonNull(config);
    }

    MaintenanceResult runCycle(RegisteredTable table) {
        LakeSnapshotId pinned = catalog.readHorizon(table.id()).snapshot();
        UUID opId = UUID.randomUUID();
        MaintenanceResult result = lake
                .table(new CommitterInitContext(table.id(), table.lakeTableRef()),
                        new ColdTableSpec(table.primaryKeyCols(), table.tierKeyCol()))
                .maintain(config, pinned,
                        LakeTieringProps.snapshotProps(opId,
                                LakeTieringProps.OP_KIND_MAINTENANCE,
                                LakeTieringProps.COMMIT_USER_MAINTENANCE, table.id()));
        if (!result.isNoop()) {
            catalog.logOpPhase(opId, table.id(), TieringOp.KIND_MAINTENANCE,
                    TieringOp.PHASE_ADVANCED, null,
                    "{\"rewritten\":" + result.rewrittenFiles()
                            + ",\"added\":" + result.addedFiles()
                            + ",\"expired_snapshots\":" + result.expiredSnapshots() + "}");
        }
        return result;
    }
}
