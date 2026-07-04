package io.modak.compaction;

import io.modak.catalog.Catalog;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TieringOp;
import io.modak.common.DeltaBatch;
import io.modak.common.OpKind;
import io.modak.common.OpPhase;
import io.modak.common.TableId;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringProps;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * One cycle: select a delta batch, fold it into the cold base as one snapshot,
 * advance {@code S} and clear the folded rows atomically. Read-pins block the
 * cycle (a pinned reader merges live delta over its older snapshot). Folding is
 * idempotent, so a crashed op is abandoned and simply re-folds next cycle.
 */
public final class CompactionWorker {

    private final Catalog catalog;
    private final LakeStorage lake;
    private final CompactionPolicy policy;

    public CompactionWorker(Catalog catalog, LakeStorage lake, CompactionPolicy policy) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
        this.policy = Objects.requireNonNull(policy);
    }

    public void runCycle(TableId table, Instant now) throws IOException {
        abandonStaleOps(table);

        if (catalog.pinnedHorizon(table).isPresent()) {
            return;
        }
        Optional<DeltaBatch> maybeBatch = policy.selectForCompaction(table, now);
        if (maybeBatch.isEmpty()) {
            return;
        }
        DeltaBatch batch = maybeBatch.get();
        RegisteredTable meta = catalog.get(table)
                .orElseThrow(() -> new IllegalStateException("table not registered: " + table));

        UUID opId = UUID.randomUUID();
        catalog.logOpPhase(opId, table, OpKind.COMPACTION, OpPhase.FLUSHING,
                null, "{\"entries\":" + batch.size() + "}");

        LakeCommitResult result = lake
                .table(new CommitterInitContext(table, meta.lakeTableRef()),
                        new ColdTableSpec(meta.primaryKeyCols(), meta.tierKeyCol()))
                .mergeWriter()
                .applyDelta(batch,
                        LakeTieringProps.snapshotProps(opId, OpKind.COMPACTION, table));

        catalog.logOpPhase(opId, table, OpKind.COMPACTION, OpPhase.COMMITTED,
                result.readable(), null);

        catalog.publishCompaction(table, result.readable(), batch, result.publishProps());
        catalog.logOpPhase(opId, table, OpKind.COMPACTION, OpPhase.ADVANCED,
                null, null);
    }

    private void abandonStaleOps(TableId table) {
        for (TieringOp op : catalog.findIncompleteOps(table, OpKind.COMPACTION)) {
            catalog.logOpPhase(op.opId(), table, OpKind.COMPACTION,
                    OpPhase.ABANDONED, null, null);
        }
    }
}
