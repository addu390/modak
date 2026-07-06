package io.tierdb.common;

import java.util.List;

/**
 * The concrete {@link PartitionData}, a fully-materialized batch of rows with
 * a small, portable column vocabulary ({@link ColumnType}). Values are
 * positional per {@link #columns()}, with {@code null}s allowed.
 */
public record RowBatchData(
        PartitionId partitionId,
        PartitionBounds bounds,
        List<Column> columns,
        List<Object[]> rows) implements PartitionData {

    /** One column, name plus portable type. Precision/scale are meaningful for DECIMAL only. */
    public record Column(String name, ColumnType type, int precision, int scale) {
        public Column(String name, ColumnType type) {
            this(name, type, 0, 0);
        }
    }

    /** Portable scalar vocabulary, anything beyond these is carried as text. */
    public enum ColumnType { LONG, DOUBLE, BOOLEAN, TEXT, TIMESTAMP, DATE, DECIMAL, UUID, BINARY }

    public RowBatchData {
        columns = List.copyOf(columns);
        rows = List.copyOf(rows);
    }

    @Override
    public long rowCount() {
        return rows.size();
    }
}
