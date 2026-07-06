package io.modak.lake.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

/** Metadata-only scan queries shared by retention and ingest. */
final class IcebergScans {

    private IcebergScans() {}

    static boolean anyFileMatches(Table table, Expression filter) {
        if (table.currentSnapshot() == null) {
            return false;
        }
        try (CloseableIterable<FileScanTask> tasks =
                table.newScan().filter(filter).planFiles()) {
            return tasks.iterator().hasNext();
        } catch (IOException e) {
            throw new UncheckedIOException("file scan probe failed", e);
        }
    }
}
