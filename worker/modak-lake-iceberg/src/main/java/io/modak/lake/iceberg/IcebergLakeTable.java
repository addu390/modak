package io.modak.lake.iceberg;

import io.modak.common.LakeSnapshotId;
import io.modak.common.RowBatchData.Column;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeTable;
import io.modak.lake.MaintenanceConfig;
import io.modak.lake.MaintenanceResult;
import io.modak.lake.MergeWriter;
import io.modak.lake.TierKeyWindow;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

/**
 * Iceberg implementation of {@link LakeTable}, one loaded table plus its
 * {@link ColdTableSpec}. Small operations commit here through the shared
 * epilogue, while the ingest and staging algorithms live in their own collaborators.
 */
final class IcebergLakeTable implements LakeTable {

    private final Table table;
    private final ColdTableSpec spec;
    private final IcebergSnapshotCommit commit;

    IcebergLakeTable(Table table, ColdTableSpec spec) {
        this.table = table;
        this.spec = spec;
        this.commit = new IcebergSnapshotCommit(table);
    }

    @Override
    public MergeWriter mergeWriter() {
        return new IcebergMergeWriter(table);
    }

    @Override
    public void evolveSchema(List<Column> addColumns) {
        new IcebergSchemaEvolution(table).addMissing(addColumns);
    }

    @Override
    public MaintenanceResult maintain(MaintenanceConfig config,
            LakeSnapshotId oldestPinnedSnapshot, Map<String, String> snapshotProps) {
        try {
            return new IcebergMaintenance(table)
                    .run(config, oldestPinnedSnapshot.id(), snapshotProps);
        } catch (Exception e) {
            throw new IllegalStateException("maintenance failed for " + table.name(), e);
        }
    }

    @Override
    public LakeCommitResult expireBelow(long boundary, Map<String, String> snapshotProps) {
        table.refresh();
        Expression below = Expressions.lessThan(spec.tierKeyCol(), boundary);
        if (!IcebergScans.anyFileMatches(table, below)) {
            return null;
        }
        DeleteFiles delete = table.newDelete().deleteFromRowFilter(below);
        return commit.apply(delete, snapshotProps);
    }

    @Override
    public LakeCommitResult ingest(List<String> files, TierKeyWindow window,
            Map<String, String> snapshotProps) {
        return new IcebergIngest(table, spec).ingest(files, window, snapshotProps);
    }

    @Override
    public List<String> stageRows(List<String> columns, Iterable<Object[]> rows) {
        return new IcebergRecordStage(table).stage(columns, rows);
    }
}
