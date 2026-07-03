package io.modak.lake;

import io.modak.common.RowBatchData.Column;
import java.util.List;
import java.util.Set;

/**
 * Facade for one cold-store format: table naming and lifecycle, plus factories
 * for the tiering pipeline and per-table handles. Everything above this
 * interface is format-agnostic.
 */
public interface LakeStorage {

    /**
     * The format's name for a table's cold counterpart, a warehouse path or a
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

    LakeSnapshotReader snapshotReader();

    /** A handle for per-table operations, see {@link LakeTable}. */
    LakeTable table(CommitterInitContext context, ColdTableSpec spec);
}
