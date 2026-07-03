package io.modak.worker;

import io.modak.catalog.Catalog;
import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.catalog.TieringOp;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringProps;
import java.util.HashMap;
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
        long line = catalog.readRetentionLine(table.id())
                .map(TierKey::value).orElse(Long.MIN_VALUE);
        if (boundary <= line) {
            return;
        }

        UUID opId = UUID.randomUUID();
        catalog.logOpPhase(opId, table.id(), TieringOp.KIND_RETENTION, TieringOp.PHASE_FLUSHING,
                null, "{\"boundary\":" + boundary + "}");

        LakeCommitResult result = lake.expireBelow(
                new CommitterInitContext(table.id(), table.lakeTableRef()),
                table.tierKeyCol(), boundary, snapshotProps(table.id(), opId));

        if (result == null) {
            catalog.publishRetention(table.id(), null, new TierKey(boundary), Map.of());
        } else {
            catalog.logOpPhase(opId, table.id(), TieringOp.KIND_RETENTION,
                    TieringOp.PHASE_COMMITTED, result.readable(), null);
            catalog.publishRetention(table.id(), result.readable(), new TierKey(boundary),
                    result.publishProps());
            Log.info("%s.%s: retention expired lake rows below %d",
                    table.schemaName(), table.tableName(), boundary);
        }
        catalog.logOpPhase(opId, table.id(), TieringOp.KIND_RETENTION, TieringOp.PHASE_ADVANCED,
                null, null);
    }

    // R must never pass a partition whose heap rows still exist: reads below the
    // cut-line come from the lake, so expiring them would answer with nothing.
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
        for (TieringOp op : catalog.findIncompleteOps(table, TieringOp.KIND_RETENTION)) {
            catalog.logOpPhase(op.opId(), table, TieringOp.KIND_RETENTION,
                    TieringOp.PHASE_ABANDONED, null, null);
        }
    }

    private static Map<String, String> snapshotProps(TableId table, UUID opId) {
        Map<String, String> props = new HashMap<>();
        props.put(LakeTieringProps.OP_ID, opId.toString());
        props.put(LakeTieringProps.OP_KIND, LakeTieringProps.OP_KIND_RETENTION);
        props.put(LakeTieringProps.TABLE_ID, Long.toString(table.oid()));
        props.put(LakeTieringProps.COMMIT_USER, LakeTieringProps.COMMIT_USER_RETENTION);
        return props;
    }
}
