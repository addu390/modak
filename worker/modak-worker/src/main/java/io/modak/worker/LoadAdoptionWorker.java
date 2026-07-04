package io.modak.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.modak.catalog.Catalog;
import io.modak.catalog.LoadLabel;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.catalog.TieringOp;
import io.modak.common.OpKind;
import io.modak.common.OpPhase;
import io.modak.common.TierKey;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommittedLakeSnapshot;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeCommitter;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringProps;
import io.modak.lake.TierKeyWindow;
import io.modak.load.StagedFiles;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Adopts staged Stream Load parquet: all staged labels per table become one
 * ingest commit per cycle, then {@code S} advances and the labels flip.
 */
final class LoadAdoptionWorker {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Catalog catalog;
    private final LakeStorage lake;

    LoadAdoptionWorker(Catalog catalog, LakeStorage lake) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
    }

    void runCycle(RegisteredTable table) {
        // Fully mirrored tables never spool, the mirror pump owns the lake.
        if (table.mode() == TableMode.MIRRORED && table.heapRetentionLag().isEmpty()) {
            return;
        }
        resume(table);
        List<LoadLabel> staged = catalog.stagedLoads(table.id());
        if (staged.isEmpty()) {
            return;
        }
        adopt(table, staged);
    }

    private void adopt(RegisteredTable table, List<LoadLabel> staged) {
        List<String> labels = staged.stream().map(LoadLabel::label).toList();
        List<String> files = new ArrayList<>();
        for (LoadLabel label : staged) {
            StagedFiles.fromJson(label.stagedFilesJson())
                    .ifPresent(m -> files.addAll(m.files()));
        }

        UUID opId = UUID.randomUUID();
        catalog.logOpPhase(opId, table.id(), OpKind.LOAD, OpPhase.FLUSHING,
                null, opDetails(labels, files.size()));

        LakeCommitResult result = lake.table(
                        new CommitterInitContext(table.id(), table.lakeTableRef()),
                        new ColdTableSpec(table.primaryKeyCols(), table.tierKeyCol()))
                .ingest(files, coldWindow(table),
                        LakeTieringProps.snapshotProps(opId, OpKind.LOAD, table.id()));

        if (result == null) {
            // Nothing to commit, the labels still flip.
            catalog.finishLoad(table.id(), labels,
                    catalog.readCutline(table.id()).snapshot(), java.util.Map.of());
            catalog.logOpPhase(opId, table.id(), OpKind.LOAD,
                    OpPhase.ADVANCED, null, null);
            return;
        }
        catalog.logOpPhase(opId, table.id(), OpKind.LOAD, OpPhase.COMMITTED,
                result.readable(), null);
        catalog.finishLoad(table.id(), labels, result.readable(), result.publishProps());
        catalog.logOpPhase(opId, table.id(), OpKind.LOAD, OpPhase.ADVANCED,
                null, null);
        Log.info("%s.%s: adopted %d staged load(s) (%d file(s)) in one commit",
                table.schemaName(), table.tableName(), labels.size(), files.size());
    }

    // A committed-but-unpublished op completes from the snapshot's stamps,
    // anything else is abandoned and its labels re-adopt (the ingest is an upsert).
    private void resume(RegisteredTable table) {
        List<TieringOp> pending = catalog.findIncompleteOps(table.id(), OpKind.LOAD);
        if (pending.isEmpty()) {
            return;
        }
        Optional<CommittedLakeSnapshot> missing = probeMissingSnapshot(table);
        for (TieringOp op : pending) {
            boolean thisOpCommitted = missing.isPresent()
                    && op.opId().toString().equals(
                            missing.get().snapshotProps().get(LakeTieringProps.OP_ID));
            if (thisOpCommitted) {
                catalog.finishLoad(table.id(), opLabels(op), missing.get().readable(),
                        missing.get().publishProps());
                catalog.logOpPhase(op.opId(), table.id(), OpKind.LOAD,
                        OpPhase.ADVANCED, missing.get().readable(), null);
                Log.info("%s.%s: resumed load adoption op %s from the lake",
                        table.schemaName(), table.tableName(), op.opId());
            } else {
                catalog.logOpPhase(op.opId(), table.id(), OpKind.LOAD,
                        OpPhase.ABANDONED, null, null);
            }
        }
    }

    private Optional<CommittedLakeSnapshot> probeMissingSnapshot(RegisteredTable table) {
        try (LakeCommitter<?, ?> committer = lake.tieringFactory().createCommitter(
                new CommitterInitContext(table.id(), table.lakeTableRef()))) {
            return committer.getMissingLakeSnapshot(
                    catalog.readCutline(table.id()).snapshot(), OpKind.LOAD);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "missing-snapshot probe failed for " + table.id(), e);
        }
    }

    /** The cold window [R, T): retention line (or open) up to the cut-line. */
    private TierKeyWindow coldWindow(RegisteredTable table) {
        long t = catalog.readCutline(table.id()).t().value();
        long r = catalog.readRetentionLine(table.id())
                .map(TierKey::value).orElse(Long.MIN_VALUE);
        return new TierKeyWindow(r, t);
    }

    private static String opDetails(List<String> labels, int files) {
        ObjectNode node = MAPPER.createObjectNode();
        ArrayNode arr = node.putArray("labels");
        labels.forEach(arr::add);
        node.put("files", files);
        return node.toString();
    }

    private static List<String> opLabels(TieringOp op) {
        try {
            JsonNode node = MAPPER.readTree(op.detailsJson());
            List<String> labels = new ArrayList<>();
            node.path("labels").forEach(l -> labels.add(l.asText()));
            return labels;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "unreadable details for load op " + op.opId(), e);
        }
    }
}
