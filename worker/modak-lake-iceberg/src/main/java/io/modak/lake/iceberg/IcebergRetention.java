package io.modak.lake.iceberg;

import io.modak.common.LakeSnapshotId;
import io.modak.lake.LakeCommitResult;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Drops every data file below the retention boundary as one metadata-only commit.
 * The boundary must align with the table's truncate(tier_key) layout: a file that
 * straddles it fails Iceberg's whole-file validation instead of rewriting rows.
 */
final class IcebergRetention {

    private IcebergRetention() {}

    static LakeCommitResult expireBelow(Table table, String tierKeyCol, long boundary,
            Map<String, String> snapshotProps) throws IOException {
        table.refresh();
        if (table.currentSnapshot() == null) {
            return null;
        }
        Expression below = Expressions.lessThan(tierKeyCol, boundary);
        if (!anyFileMatches(table, below)) {
            return null;
        }
        DeleteFiles delete = table.newDelete().deleteFromRowFilter(below);
        snapshotProps.forEach(delete::set);
        delete.commit();
        table.refresh();
        return LakeCommitResult.committedIsReadable(
                new LakeSnapshotId(table.currentSnapshot().sequenceNumber()),
                IcebergPublish.props(table));
    }

    private static boolean anyFileMatches(Table table, Expression filter) throws IOException {
        try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
            return tasks.iterator().hasNext();
        }
    }
}
