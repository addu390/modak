package io.modak.lake;

import io.modak.common.LakeSnapshotId;
import io.modak.common.RowBatchData.Column;
import java.util.List;
import java.util.Map;

/**
 * A handle on one cold table, obtained from {@link LakeStorage#table}. It
 * already knows the table and its {@link ColdTableSpec}, so operations state
 * only what varies per call.
 */
public interface LakeTable {

    MergeWriter mergeWriter();

    /**
     * Adds the given columns (as optional fields) to the lake table's schema
     * when absent. Idempotent, so safe across crash/replay.
     */
    void evolveSchema(List<Column> addColumns);

    /**
     * One maintenance pass: bin-pack small data files and expire old snapshots,
     * never any at or above {@code oldestPinnedSnapshot}, because expiry deletes
     * data files and would break a pinned read.
     */
    MaintenanceResult maintain(MaintenanceConfig config, LakeSnapshotId oldestPinnedSnapshot,
            Map<String, String> snapshotProps);

    /**
     * Deletes every lake row with {@code tier_key < boundary} as one commit and
     * returns it, or {@code null} when nothing lies below the boundary. The
     * boundary must be partition-aligned so the delete stays file-aligned.
     */
    LakeCommitResult expireBelow(long boundary, Map<String, String> snapshotProps);

    /**
     * Commits externally staged data files as one atomic upsert, where a
     * staged row whose pk exists replaces it. Every file's tier-key range must
     * lie in the window. One file outside it fails the whole call and nothing commits.
     */
    LakeCommitResult ingest(List<String> files, TierKeyWindow window,
            Map<String, String> snapshotProps);

    /**
     * Writes rows into the cold table's own storage as staged data files for a
     * subsequent {@link #ingest}. Values are coerced to the lake schema by
     * column name and files never straddle a partition boundary. Commits nothing.
     */
    List<String> stageRows(List<String> columns, Iterable<Object[]> rows);
}
