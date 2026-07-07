package io.tierdb.load;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.ColdTableSpec;
import io.tierdb.lake.LakePartition;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.LakeSnapshotReader;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.LakeTable;
import io.tierdb.lake.commit.LakeTieringFactory;
import io.tierdb.lake.commit.MergeWriter;
import io.tierdb.lake.maintain.MaintenancePlan;
import io.tierdb.lake.maintain.MaintenanceResult;
import io.tierdb.lake.TierKeyWindow;
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
    public Map<String, String> createTableIfAbsent(String ref, List<Column> columns,
            java.util.Set<String> requiredCols, String tierKeyCol,
            LakePartition partition) {
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
    public MergeWriter mergeWriter() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void evolveSchema(List<Column> addColumns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public MaintenanceResult maintain(MaintenancePlan plan, Map<String, String> snapshotProps) {
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
