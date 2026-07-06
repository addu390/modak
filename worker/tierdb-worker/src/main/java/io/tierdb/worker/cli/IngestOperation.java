package io.tierdb.worker.cli;

import io.tierdb.catalog.Catalog;
import io.tierdb.catalog.CatalogException;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.TableMode;
import io.tierdb.common.Cutline;
import io.tierdb.common.OpKind;
import io.tierdb.common.OpPhase;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.TierKey;
import io.tierdb.lake.ColdTableSpec;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.LakeTable;
import io.tierdb.lake.TierKeyWindow;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.commit.LakeTieringProps;
import io.tierdb.worker.Log;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Bulk ingestion: commits cold rows straight into the lake as one atomic
 * upsert, bypassing {@code tierdb.delta}, then journals and advances the pinned
 * snapshot. Applies to tiered tables and mirrored tables with heap retention.
 */
public final class IngestOperation {

    private final Catalog catalog;
    private final LakeStorage lake;

    public IngestOperation(Catalog catalog, LakeStorage lake) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
    }

    public List<String> stage(RegisteredTable table, List<Column> columns, Path jsonl) {
        JsonlRows rows = new JsonlRows(jsonl, columns, table.tierKeyCol(), coldWindow(table));
        rows.validate();
        if (rows.isEmpty()) {
            return List.of();
        }
        return lakeTable(table).stageRows(rows.columnNames(), rows);
    }

    public void ingest(RegisteredTable table, List<String> files) {
        if (table.mode() == TableMode.MIRRORED && table.heapRetentionLag().isEmpty()) {
            throw new IllegalArgumentException("bulk ingest applies to tiered tables and "
                    + "mirrored tables with heap retention: a fully mirrored heap is the "
                    + "source of truth, so bulk data must go through Postgres");
        }
        TierKeyWindow window = coldWindow(table);

        UUID opId = UUID.randomUUID();
        catalog.logOpPhase(opId, table.id(), OpKind.INGEST, OpPhase.FLUSHING,
                null, "{\"files\":" + files.size() + "}");

        LakeCommitResult result = lakeTable(table).ingest(files, window,
                LakeTieringProps.snapshotProps(opId, OpKind.INGEST, table.id()));
        if (result == null) {
            catalog.logOpPhase(opId, table.id(), OpKind.INGEST,
                    OpPhase.ABANDONED, null, null);
            return;
        }
        catalog.logOpPhase(opId, table.id(), OpKind.INGEST, OpPhase.COMMITTED,
                result.readable(), null);
        advanceSnapshot(table, result);
        catalog.logOpPhase(opId, table.id(), OpKind.INGEST, OpPhase.ADVANCED,
                null, null);
        Log.info("%s.%s: ingested %d file(s) below T=%d",
                table.schemaName(), table.tableName(), files.size(), window.maxExclusive());
    }

    private TierKeyWindow coldWindow(RegisteredTable table) {
        long t = catalog.readCutline(table.id()).t().value();
        long r = catalog.readRetentionLine(table.id())
                .map(TierKey::value).orElse(Long.MIN_VALUE);
        return new TierKeyWindow(r, t);
    }

    private LakeTable lakeTable(RegisteredTable table) {
        return lake.table(new CommitterInitContext(table.id(), table.lakeTableRef()),
                new ColdTableSpec(table.primaryKeyCols(), table.tierKeyCol()));
    }

    private void advanceSnapshot(RegisteredTable table, LakeCommitResult result) {
        Cutline current = catalog.readCutline(table.id());
        if (current.snapshot().compareTo(result.readable()) >= 0) {
            return;
        }
        try {
            catalog.advanceCutline(table.id(), current.t(), result.readable(),
                    result.publishProps());
        } catch (CatalogException e) {
            Log.info("%s.%s: S already advanced past the ingest snapshot",
                    table.schemaName(), table.tableName());
        }
    }
}
