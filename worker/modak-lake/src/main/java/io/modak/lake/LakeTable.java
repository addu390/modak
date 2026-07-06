package io.modak.lake;

import io.modak.common.LakeSnapshotId;
import io.modak.common.RowBatchData.Column;
import io.modak.lake.commit.LakeCommitResult;
import io.modak.lake.commit.MergeWriter;
import io.modak.lake.maintain.MaintenanceEngine;
import io.modak.lake.maintain.MaintenancePlan;
import io.modak.lake.maintain.MaintenanceResult;
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
