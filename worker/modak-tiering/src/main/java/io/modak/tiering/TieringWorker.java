package io.modak.tiering;

import io.modak.catalog.Catalog;
import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TieringOp;
import io.modak.common.Cutline;
import io.modak.common.LakeSnapshotId;
import io.modak.common.OpKind;
import io.modak.common.OpPhase;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.lake.CommittedLakeSnapshot;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeCommitter;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringFactory;
import io.modak.lake.LakeTieringProps;
import io.modak.lake.LakeWriter;
import io.modak.lake.WriterInitContext;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The aging-down loop. Resume, seal, flush, commit ONE lake snapshot per batch,
 * advance {@code (T, S, lake_props)} atomically, reclaim below the pinned-reader
 * horizon. The lake commit is the point of no return. Before it a crash abandons
 * the op, after it resume completes the advance from the snapshot's own stamps.
 */
public final class TieringWorker {

    private final Catalog catalog;
    private final LakeStorage lake;
    private final HotSource hotSource;
    private final TieringPolicy tieringPolicy;
    private final EvictionPolicy evictionPolicy;

    public TieringWorker(Catalog catalog, LakeStorage lake, HotSource hotSource,
            TieringPolicy tieringPolicy, EvictionPolicy evictionPolicy) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
        this.hotSource = Objects.requireNonNull(hotSource);
        this.tieringPolicy = Objects.requireNonNull(tieringPolicy);
        this.evictionPolicy = Objects.requireNonNull(evictionPolicy);
    }

    public void runCycle(TableId table, Instant now) {
        RegisteredTable meta = catalog.get(table)
                .orElseThrow(() -> new TieringException("table not registered: " + table));

        resume(meta);

        List<PartitionId> selected = tieringPolicy.selectForTiering(table, now);
        if (!selected.isEmpty()) {
            tier(meta, selected);
        }
        reclaim(meta);
    }

    private void tier(RegisteredTable meta, List<PartitionId> selected) {
        TableId table = meta.id();
        Cutline current = catalog.readCutline(table);
        List<PartitionInfo> parts = orderedContiguousBatch(table, selected, current.t());
        TierKey newT = parts.get(parts.size() - 1).bounds().hi();

        UUID opId = UUID.randomUUID();
        catalog.logOpPhase(opId, table, OpKind.TIERING, OpPhase.FLUSHING,
                null, opDetails(parts, newT));

        for (PartitionInfo p : parts) {
            ensureTiering(p);
        }

        LakeTieringFactory<?, ?> factory = lake.tieringFactory();
        Map<String, Long> rowsByPartition = new HashMap<>();
        LakeCommitResult result = flushAndCommit(factory, meta, parts, opId, newT, rowsByPartition);

        if (result == null) {
            // Empty flush: T must still advance or the policy re-selects these partitions forever.
            catalog.advanceCutline(table, newT, catalog.readCutline(table).snapshot(), Map.of());
            catalog.logOpPhase(opId, table, OpKind.TIERING,
                    OpPhase.ADVANCED, null, null);
        } else {
            catalog.logOpPhase(opId, table, OpKind.TIERING, OpPhase.COMMITTED,
                    result.readable(), opDetails(parts, newT, rowsByPartition));

            catalog.advanceCutline(table, newT, result.readable(), result.publishProps());
            catalog.logOpPhase(opId, table, OpKind.TIERING, OpPhase.ADVANCED,
                    null, null);
        }

        for (PartitionInfo p : parts) {
            catalog.transition(p.id(), PartitionState.TIERING, PartitionState.TIERED);
        }
    }

    private <W, C> LakeCommitResult flushAndCommit(LakeTieringFactory<W, C> factory,
            RegisteredTable meta, List<PartitionInfo> parts, UUID opId, TierKey newT,
            Map<String, Long> rowsByPartition) {
        TableId table = meta.id();
        try {
            List<W> results = new ArrayList<>(parts.size());
            for (PartitionInfo p : parts) {
                try (LakeWriter<W> writer = factory.createWriter(
                        new WriterInitContext(table, p.id(), p.bounds(), meta.lakeTableRef()))) {
                    var data = hotSource.read(meta, p);
                    if (data.rowCount() >= 0) {
                        rowsByPartition.put(p.id().id(), data.rowCount());
                    }
                    writer.write(data);
                    results.add(writer.complete());
                }
            }
            try (LakeCommitter<W, C> committer =
                    factory.createCommitter(new CommitterInitContext(table, meta.lakeTableRef()))) {
                C committable = committer.toCommittable(results);
                // A lake snapshot the catalog doesn't know would be duplicated by
                // committing on top: backfill from its stamps, abort, re-tier next cycle.
                Optional<CommittedLakeSnapshot> missing =
                        committer.getMissingLakeSnapshot(catalog.readCutline(table).snapshot());
                if (missing.isPresent() && belongsTo(table, missing.get().snapshotProps())) {
                    if (committable != null) {
                        committer.abort(committable);
                    }
                    advanceFromSnapshot(meta, missing.get());
                    catalog.logOpPhase(opId, table, OpKind.TIERING,
                            OpPhase.ABANDONED, null, null);
                    throw new TieringException("catalog was behind the lake for " + table
                            + ", backfilled from the lake and aborted op " + opId
                            + ", the next cycle re-tiers cleanly");
                }
                if (committable == null) {
                    return null;
                }
                try {
                    return committer.commit(committable, snapshotProps(table, opId, newT));
                } catch (IOException | RuntimeException e) {
                    committer.abort(committable);
                    throw e;
                }
            }
        } catch (TieringException e) {
            throw e;
        } catch (Exception e) {
            throw new TieringException("tiering op " + opId + " failed for " + table
                    + " (resumable: data is either uncommitted or recoverable from the lake)", e);
        }
    }

    static Map<String, String> snapshotProps(TableId table, UUID opId, TierKey newT) {
        Map<String, String> props = LakeTieringProps.snapshotProps(opId, OpKind.TIERING, table);
        props.put(LakeTieringProps.NEW_TIER_KEY_HI, Long.toString(newT.value()));
        return props;
    }

    // A stamped mismatch means a previous incarnation of a dropped+re-registered table.
    private static boolean belongsTo(TableId table, Map<String, String> snapshotProps) {
        String stamped = snapshotProps.get(LakeTieringProps.TABLE_ID);
        return stamped == null || stamped.equals(Long.toString(table.oid()));
    }

    private void resume(RegisteredTable meta) {
        TableId table = meta.id();
        List<TieringOp> pending = catalog.findIncompleteOps(table, OpKind.TIERING);
        if (pending.isEmpty()) {
            return;
        }
        Optional<CommittedLakeSnapshot> missing = probeMissingSnapshot(meta);
        for (TieringOp op : pending) {
            boolean thisOpCommitted = missing.isPresent()
                    && belongsTo(table, missing.get().snapshotProps())
                    && op.opId().toString().equals(missing.get().snapshotProps().get(LakeTieringProps.OP_ID));
            if (thisOpCommitted) {
                completeAdvance(meta, op, missing.get());
            } else {
                catalog.logOpPhase(op.opId(), table, OpKind.TIERING,
                        OpPhase.ABANDONED, null, null);
            }
        }
    }

    private Optional<CommittedLakeSnapshot> probeMissingSnapshot(RegisteredTable meta) {
        LakeSnapshotId lastKnown = catalog.readCutline(meta.id()).snapshot();
        try (LakeCommitter<?, ?> committer = lake.tieringFactory()
                .createCommitter(new CommitterInitContext(meta.id(), meta.lakeTableRef()))) {
            return committer.getMissingLakeSnapshot(lastKnown);
        } catch (Exception e) {
            throw new TieringException("missing-snapshot probe failed for " + meta.id(), e);
        }
    }

    private void completeAdvance(RegisteredTable meta, TieringOp op, CommittedLakeSnapshot found) {
        advanceFromSnapshot(meta, found);
        catalog.logOpPhase(op.opId(), meta.id(), OpKind.TIERING,
                OpPhase.ADVANCED, found.readable(), null);
    }

    // Realigns the catalog with an unknown lake snapshot from its stamped properties alone.
    private void advanceFromSnapshot(RegisteredTable meta, CommittedLakeSnapshot found) {
        TableId table = meta.id();
        String stampedT = found.snapshotProps().get(LakeTieringProps.NEW_TIER_KEY_HI);
        if (stampedT == null) {
            throw new TieringException("committed snapshot "
                    + found.snapshotProps().get(LakeTieringProps.OP_ID)
                    + " lacks " + LakeTieringProps.NEW_TIER_KEY_HI + ", cannot advance safely");
        }
        TierKey newT = new TierKey(Long.parseLong(stampedT));

        catalog.advanceCutline(table, newT, found.readable(), found.publishProps());

        // The op's partitions are exactly those wholly below the new T and mid-flight.
        for (PartitionInfo p : catalog.listPartitions(table)) {
            if (p.bounds().hi().compareTo(newT) <= 0) {
                if (p.state() == PartitionState.SEALING) {
                    catalog.transition(p.id(), PartitionState.SEALING, PartitionState.TIERING);
                }
                if (p.state() != PartitionState.TIERED && p.state() != PartitionState.DROPPED) {
                    catalog.transition(p.id(), PartitionState.TIERING, PartitionState.TIERED);
                }
            }
        }
    }

    private void reclaim(RegisteredTable meta) {
        try {
            Cutline horizon = catalog.readHorizon(meta.id());
            List<PartitionInfo> parts = new ArrayList<>(catalog.listPartitions(meta.id()));
            parts.sort((a, b) -> a.bounds().lo().compareTo(b.bounds().lo()));
            for (PartitionInfo partition : parts) {
                if (evictionPolicy.canReclaim(partition, horizon)) {
                    hotSource.dropPartition(meta, partition.id());
                    catalog.transition(partition.id(), PartitionState.TIERED, PartitionState.DROPPED);
                }
            }
        } catch (RuntimeException e) {
            throw new ReclaimException("reclaim failed for " + meta.id()
                    + ", tiered data is committed and the cut-line advanced, only the "
                    + "hot-partition DROP retries next cycle", e);
        }
    }

    // A gap-free batch guarantees advancing T strands no hot row below the new cut-line.
    private List<PartitionInfo> orderedContiguousBatch(TableId table, List<PartitionId> selected,
            TierKey currentT) {
        Map<PartitionId, PartitionInfo> byId = new HashMap<>();
        for (PartitionInfo p : catalog.listPartitions(table)) {
            byId.put(p.id(), p);
        }
        List<PartitionInfo> parts = new ArrayList<>(selected.size());
        for (PartitionId id : selected) {
            PartitionInfo p = byId.get(id);
            if (p == null) {
                throw new TieringException("policy selected unknown partition: " + id);
            }
            parts.add(p);
        }
        parts.sort((a, b) -> a.bounds().lo().compareTo(b.bounds().lo()));

        if (parts.get(0).bounds().lo().compareTo(currentT) > 0) {
            throw new TieringException("batch would leave a gap below the new cut-line: starts at "
                    + parts.get(0).bounds().lo() + " but T is " + currentT);
        }
        for (int i = 1; i < parts.size(); i++) {
            if (!parts.get(i).bounds().lo().equals(parts.get(i - 1).bounds().hi())) {
                throw new TieringException("batch is not contiguous at " + parts.get(i).id());
            }
        }
        return parts;
    }

    private void ensureTiering(PartitionInfo p) {
        switch (p.state()) {
            case HOT -> {
                catalog.transition(p.id(), PartitionState.HOT, PartitionState.SEALING);
                catalog.transition(p.id(), PartitionState.SEALING, PartitionState.TIERING);
            }
            case SEALING -> catalog.transition(p.id(), PartitionState.SEALING, PartitionState.TIERING);
            case TIERING -> { /* re-driven after an abandoned op */ }
            default -> throw new TieringException(
                    "partition " + p.id() + " not tierable from state " + p.state());
        }
    }

    private static String opDetails(List<PartitionInfo> parts, TierKey newT) {
        return opDetails(parts, newT, Map.of());
    }

    private static String opDetails(List<PartitionInfo> parts, TierKey newT,
            Map<String, Long> rowsByPartition) {
        StringBuilder sb = new StringBuilder("{\"new_tier_key_hi\":").append(newT.value())
                .append(",\"partitions\":[");
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(parts.get(i).id().id().replace("\"", "\\\"")).append('"');
        }
        sb.append(']');
        if (!rowsByPartition.isEmpty()) {
            sb.append(",\"partition_rows\":{");
            boolean first = true;
            for (Map.Entry<String, Long> e : rowsByPartition.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                sb.append('"').append(e.getKey().replace("\"", "\\\"")).append("\":")
                        .append(e.getValue());
            }
            sb.append('}');
        }
        return sb.append('}').toString();
    }
}
