package io.tierdb.lake;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeTieringFactory;
import java.util.List;
import java.util.Set;

/**
 * Facade for one cold-store format: table naming and lifecycle, plus factories
 * for the tiering pipeline and per-table handles. Everything above this
 * interface is format-agnostic.
 */
public interface LakeStorage {

    String tableRef(String schema, String table);

    String createTableIfAbsent(String ref, List<Column> columns, Set<String> requiredCols,
            String tierKeyCol, LakePartition partition);

    void dropTable(String ref);

    LakeTieringFactory<?, ?> tieringFactory();

    LakeSnapshotReader snapshotReader();

    LakeTable table(CommitterInitContext context, ColdTableSpec spec);
}
