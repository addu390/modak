package io.tierdb.worker.ops;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.tierdb.catalog.Catalog;
import io.tierdb.catalog.LoadLabel;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.TableMode;
import io.tierdb.catalog.TieringOp;
import io.tierdb.common.OpKind;
import io.tierdb.common.OpPhase;
import io.tierdb.common.TierKey;
import io.tierdb.lake.ColdTableSpec;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.TierKeyWindow;
import io.tierdb.lake.commit.CommittedLakeSnapshot;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.commit.LakeCommitter;
import io.tierdb.lake.commit.LakeTieringProps;
import io.tierdb.load.StagedFiles;
import io.tierdb.worker.Log;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Adopts staged Stream Load parquet: all staged labels per table become one
 * ingest commit per cycle, then {@code S} advances and the labels flip.
 */
public final class LoadAdoptionWorker {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Catalog catalog;
    private final LakeStorage lake;

    public LoadAdoptionWorker(Catalog catalog, LakeStorage lake) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
    }

    public void runCycle(RegisteredTable table) {
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
