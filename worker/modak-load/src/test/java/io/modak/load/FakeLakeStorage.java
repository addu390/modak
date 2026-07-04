package io.modak.load;

import io.modak.common.LakeSnapshotId;
import io.modak.common.RowBatchData.Column;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeSnapshotReader;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTable;
import io.modak.lake.LakeTieringFactory;
import io.modak.lake.MaintenanceConfig;
import io.modak.lake.MaintenanceResult;
import io.modak.lake.TierKeyWindow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Captures staged rows instead of writing parquet, enough for spool tests. */
final class FakeLakeStorage implements LakeStorage, LakeTable {

    List<String> stagedColumns;
    final List<Object[]> stagedRows = new ArrayList<>();
    int stageCalls;

    @Override
    public List<String> stageRows(List<String> columns, Iterable<Object[]> rows) {
        stageCalls++;
        stagedColumns = columns;
        rows.forEach(stagedRows::add);
        return List.of("/fake/warehouse/staged-" + stageCalls + ".parquet");
    }

    @Override
    public LakeTable table(CommitterInitContext context, ColdTableSpec spec) {
        return this;
    }

    @Override
    public String tableRef(String schema, String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String createTableIfAbsent(String ref, List<Column> columns,
            java.util.Set<String> requiredCols, String tierKeyCol, long partitionWidth) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(String ref) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LakeTieringFactory<?, ?> tieringFactory() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LakeSnapshotReader snapshotReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public io.modak.lake.MergeWriter mergeWriter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void evolveSchema(List<Column> addColumns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaintenanceResult maintain(MaintenanceConfig config,
            LakeSnapshotId oldestPinnedSnapshot, Map<String, String> snapshotProps) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LakeCommitResult expireBelow(long boundary, Map<String, String> snapshotProps) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LakeCommitResult ingest(List<String> files, TierKeyWindow window,
            Map<String, String> snapshotProps) {
        throw new UnsupportedOperationException();
    }
}
