package io.modak.worker;

import io.modak.catalog.Catalog;
import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.catalog.TieringOp;
import io.modak.common.OpKind;
import io.modak.common.OpPhase;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTable;
import io.modak.lake.LakeTieringProps;
import io.modak.load.StagedFiles;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Expires lake data older than a tiered table's {@code lake_retention_lag}. The
 * boundary R = T - lag is floor-aligned to the partition width (the delete stays
 * file-aligned), clamped so no partition still holding heap rows is expired, and
 * the pass is skipped entirely while readers are pinned.
 */
final class RetentionWorker {

    private final Catalog catalog;
    private final LakeStorage lake;

    RetentionWorker(Catalog catalog, LakeStorage lake) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
    }

    void runCycle(RegisteredTable table) {
        if (table.mode() != TableMode.TIERED || table.lakeRetentionLag().isEmpty()) {
            return;
        }
        abandonStaleOps(table.id());
        long width = table.partitionWidth().orElse(0L);
        if (width <= 0) {
            return;
        }
        if (catalog.pinnedHorizon(table.id()).isPresent()) {
            return;
        }

        long t = catalog.readCutline(table.id()).t().value();
        long raw = t - table.lakeRetentionLag().get();
        long boundary = Math.floorDiv(raw, width) * width;
        boundary = Math.min(boundary, heapFrontier(table.id()));
        // R past a staged load would make its adoption unable to ever commit.
        long stagedFloor = stagedLoadFloor(table.id());
        if (stagedFloor != Long.MAX_VALUE) {
            boundary = Math.min(boundary, Math.floorDiv(stagedFloor, width) * width);
        }
        long line = catalog.readRetentionLine(table.id())
                .map(TierKey::value).orElse(Long.MIN_VALUE);
        if (boundary <= line) {
            return;
        }

        UUID opId = UUID.randomUUID();
        catalog.logOpPhase(opId, table.id(), OpKind.RETENTION, OpPhase.FLUSHING,
                null, "{\"boundary\":" + boundary + "}");

        LakeCommitResult result = lakeTable(table).expireBelow(boundary,
                LakeTieringProps.snapshotProps(opId, OpKind.RETENTION, table.id()));

        if (result == null) {
            catalog.publishRetention(table.id(), null, new TierKey(boundary), Map.of());
        } else {
            catalog.logOpPhase(opId, table.id(), OpKind.RETENTION,
                    OpPhase.COMMITTED, result.readable(), null);
            catalog.publishRetention(table.id(), result.readable(), new TierKey(boundary),
                    result.publishProps());
            Log.info("%s.%s: retention expired lake rows below %d",
                    table.schemaName(), table.tableName(), boundary);
        }
        catalog.logOpPhase(opId, table.id(), OpKind.RETENTION, OpPhase.ADVANCED,
                null, null);
    }

    private LakeTable lakeTable(RegisteredTable table) {
        return lake.table(new CommitterInitContext(table.id(), table.lakeTableRef()),
                new ColdTableSpec(table.primaryKeyCols(), table.tierKeyCol()));
    }

    private long stagedLoadFloor(TableId table) {
        long floor = Long.MAX_VALUE;
        for (var label : catalog.stagedLoads(table)) {
            floor = Math.min(floor, StagedFiles.fromJson(label.stagedFilesJson())
                    .map(StagedFiles::minTierKey).orElse(Long.MAX_VALUE));
        }
        return floor;
    }

    private long heapFrontier(TableId table) {
        long frontier = Long.MAX_VALUE;
        for (PartitionInfo p : catalog.listPartitions(table)) {
            if (p.state() != PartitionState.DROPPED) {
                frontier = Math.min(frontier, p.bounds().lo().value());
            }
        }
        return frontier;
    }

    private void abandonStaleOps(TableId table) {
        for (TieringOp op : catalog.findIncompleteOps(table, OpKind.RETENTION)) {
            catalog.logOpPhase(op.opId(), table, OpKind.RETENTION,
                    OpPhase.ABANDONED, null, null);
        }
    }
}
