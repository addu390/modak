package io.modak.lake;

import io.modak.common.LakeSnapshotId;
import io.modak.common.RowBatchData.Column;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Facade for one cold-store format: the entry point through which all format-specific
 * behaviour is reached. Everything above this interface is format-agnostic.
 */
public interface LakeStorage {

    /**
     * The format's name for a table's cold counterpart — a warehouse path or a
     * catalog identifier. Stored as {@code lake_table_ref} in {@code modak.tables}.
     */
    String tableRef(String schema, String table);

    /**
     * Creates the cold table when absent (idempotent) and returns its publishable
     * metadata location, so pinned reads work before the first commit.
     */
    String createTableIfAbsent(String ref, List<Column> columns, Set<String> requiredCols,
            String tierKeyCol, long partitionWidth);

    /** Drops the cold table and purges its data files. Idempotent: absent is a no-op. */
    void dropTable(String ref);

    LakeTieringFactory<?, ?> tieringFactory();

    MergeWriter mergeWriter(CommitterInitContext context);

    LakeSnapshotReader snapshotReader();

    /**
     * Adds the given columns (as optional fields) to the lake table's schema when
     * absent. Idempotent: columns that already exist are skipped, so the call is
     * safe across crash/replay.
     */
    void evolveSchema(CommitterInitContext context, List<Column> addColumns);

    /**
     * One maintenance pass: bin-pack small data files and expire old snapshots —
     * never any at or above {@code oldestPinnedSnapshot}, because expiry deletes
     * data files and would break a pinned read.
     */
    MaintenanceResult maintain(CommitterInitContext context, MaintenanceConfig config,
            LakeSnapshotId oldestPinnedSnapshot, Map<String, String> snapshotProps);

    /**
     * Deletes every lake row with {@code tierKeyCol < boundary} as one commit and
     * returns it, or {@code null} when nothing lies below the boundary. The boundary
     * must be aligned to the table's partition layout so the delete is file-aligned;
     * a misaligned boundary fails rather than rewriting files.
     */
    LakeCommitResult expireBelow(CommitterInitContext context, String tierKeyCol,
            long boundary, Map<String, String> snapshotProps);
}
