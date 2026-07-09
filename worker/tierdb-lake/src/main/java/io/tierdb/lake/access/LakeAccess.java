package io.tierdb.lake.access;

import io.tierdb.common.RowBatchData.Column;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The external-consumer port for one lake format: scan resolution, row
 * reading, and direct-mode merges. Deliberately smaller than
 * {@link io.tierdb.lake.LakeStorage}, the worker-side owner facade.
 */
public interface LakeAccess {

    /** The table's current snapshot, resolved live; empty when it has none. */
    Optional<LakeScan> liveScan(String tableRef);

    /**
     * A scan of the snapshot a cut-line pinned, from its published props;
     * empty when the props pin no data yet.
     */
    Optional<LakeScan> pinnedScan(Map<String, String> lakeProps);

    /** Opens the scan, yielding rows aligned to {@code columns}. */
    RowScan rows(LakeScan scan, List<String> columns, Map<Column, ColumnConstraint> filter);

    /** A merge session against the live table, for direct-mode DML. */
    LakeMerge<?> merge(String tableRef, List<String> columns, List<String> pkCols);
}
