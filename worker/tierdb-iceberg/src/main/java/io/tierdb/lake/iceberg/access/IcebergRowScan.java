package io.tierdb.lake.iceberg.access;

import io.tierdb.lake.access.RowScan;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/** Reads one Iceberg snapshot as neutral rows aligned to the requested columns. */
final class IcebergRowScan implements RowScan {

    private final List<String> columns;
    private final FileIO fileIo;
    private final CloseableIterable<Record> iterable;
    private final Iterator<Record> records;
    private Object[] next;

    IcebergRowScan(Table table, long snapshotId, FileIO fileIo, List<String> columns,
            Optional<Expression> filter) {
        this.columns = columns;
        this.fileIo = fileIo;
        IcebergGenerics.ScanBuilder scan = IcebergGenerics.read(table)
                .useSnapshot(snapshotId)
                .select(columns.toArray(String[]::new));
        if (filter.isPresent()) {
            scan = scan.where(filter.get());
        }
        this.iterable = scan.build();
        this.records = iterable.iterator();
    }

    @Override
    public boolean hasNext() {
        while (next == null && records.hasNext()) {
            Record record = records.next();
            Object[] row = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                row[i] = normalize(record.getField(columns.get(i)));
            }
            next = row;
        }
        return next != null;
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Object[] row = next;
        next = null;
        return row;
    }

    @Override
    public void close() {
        try {
            iterable.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            fileIo.close();
        }
    }

    private static Object normalize(Object value) {
        if (value instanceof LocalDateTime ldt) {
            return ldt.atOffset(ZoneOffset.UTC);
        }
        if (value instanceof ByteBuffer buf) {
            byte[] bytes = new byte[buf.remaining()];
            buf.duplicate().get(bytes);
            return bytes;
        }
        return value;
    }
}
