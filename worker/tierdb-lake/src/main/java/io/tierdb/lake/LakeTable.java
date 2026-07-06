package io.tierdb.lake;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.commit.MergeWriter;
import io.tierdb.lake.maintain.MaintenancePlan;
import io.tierdb.lake.maintain.MaintenanceResult;
import java.util.List;
import java.util.Map;

/**
 * A handle on one cold table, obtained from {@link LakeStorage#table}. It
 * already knows the table and its {@link ColdTableSpec}, so operations state
 * only what varies per call.
 */
public interface LakeTable {

    MergeWriter mergeWriter();

    default LakeStats stats() {
        return LakeStats.EMPTY;
    }

    void evolveSchema(List<Column> addColumns);

    MaintenanceResult maintain(MaintenancePlan plan, Map<String, String> snapshotProps);

    default void deleteFiles(List<String> paths) {
        throw new UnsupportedOperationException();
    }

    LakeCommitResult expireBelow(long boundary, Map<String, String> snapshotProps);

    LakeCommitResult ingest(List<String> files, TierKeyWindow window,
            Map<String, String> snapshotProps);

    List<String> stageRows(List<String> columns, Iterable<Object[]> rows);
}
