package io.tierdb.lake.iceberg.access;

import io.tierdb.lake.access.LakeMerge;
import io.tierdb.lake.access.MergeFileWriter;
import io.tierdb.lake.access.MergeWriterFactory;
import io.tierdb.lake.iceberg.IcebergValues;
import io.tierdb.lake.iceberg.TierKeys;
import io.tierdb.lake.iceberg.commit.IcebergRecordMerge;
import io.tierdb.lake.iceberg.commit.IcebergRecordMerge.MergeFiles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

/**
 * One merge against a live Iceberg table: distributable writers over the shared
 * {@link IcebergRecordMerge} core, committed as one {@code RowDelta}.
 */
final class IcebergLakeMerge implements LakeMerge<MergeFiles> {

    private final Table table;
    private final Factory factory;

    IcebergLakeMerge(Table table, List<String> columnNames, List<String> pkCols) {
        this.table = table;
        this.factory = new Factory(
                SerializableTable.copyOf(table), List.copyOf(columnNames), List.copyOf(pkCols));
    }

    @Override
    public MergeWriterFactory<MergeFiles> writerFactory() {
        return factory;
    }

    @Override
    public void commit(List<MergeFiles> results) throws IOException {
        try {
            table.refresh();
            RowDelta rowDelta = table.newRowDelta();
            for (MergeFiles files : results) {
                files.deleteFiles().forEach(rowDelta::addDeletes);
                files.dataFiles().forEach(rowDelta::addRows);
            }
            rowDelta.commit();
        } catch (RuntimeException e) {
            abort(results);
            throw new IOException(
                    "failed to commit merge into Iceberg table " + table.name(), e);
        }
    }

    @Override
    public void abort(List<MergeFiles> results) {
        List<String> orphaned = new ArrayList<>();
        for (MergeFiles files : results) {
            files.deleteFiles().forEach(f -> orphaned.add(f.path().toString()));
            files.dataFiles().forEach(f -> orphaned.add(f.path().toString()));
        }
        CatalogUtil.deleteFiles(table.io(), orphaned, "orphaned merge file", true);
    }

    private record Factory(Table table, List<String> columnNames, List<String> pkCols)
            implements MergeWriterFactory<MergeFiles> {

        @Override
        public MergeFileWriter<MergeFiles> newWriter() throws IOException {
            return new Writer(table, columnNames, pkCols);
        }
    }

    private static final class Writer implements MergeFileWriter<MergeFiles> {

        private final IcebergRecordMerge merge;
        private final List<String> columnNames;
        private final Types.NestedField[] fields;
        private final int tierKeyIndex;

        Writer(Table table, List<String> columnNames, List<String> pkCols) throws IOException {
            this.merge = new IcebergRecordMerge(table, pkCols,
                    ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE), System.nanoTime());
            this.columnNames = columnNames;
            Schema schema = merge.schema();
            this.fields = new Types.NestedField[columnNames.size()];
            for (int i = 0; i < columnNames.size(); i++) {
                fields[i] = schema.findField(columnNames.get(i));
                if (fields[i] == null) {
                    throw new IOException("column '" + columnNames.get(i)
                            + "' has no counterpart in the Iceberg schema of " + table.name());
                }
            }
            PartitionSpec spec = table.spec();
            this.tierKeyIndex = spec.isUnpartitioned()
                    ? -1
                    : columnNames.indexOf(
                            schema.findColumnName(spec.fields().get(0).sourceId()));
        }

        @Override
        public void upsert(Object[] row) throws IOException {
            long tierKey = tierKeyOf(row);
            merge.upsert(record(row), tierKey, tierKey);
        }

        @Override
        public void delete(Object[] row) throws IOException {
            merge.delete(record(row), tierKeyOf(row));
        }

        @Override
        public MergeFiles complete() throws IOException {
            return merge.complete();
        }

        @Override
        public void close() throws IOException {
            merge.close();
        }

        private Record record(Object[] row) {
            GenericRecord record = GenericRecord.create(merge.schema());
            for (int i = 0; i < fields.length; i++) {
                record.setField(columnNames.get(i),
                        IcebergValues.coerce(row[i], fields[i].type()));
            }
            return record;
        }

        private long tierKeyOf(Object[] row) throws IOException {
            if (tierKeyIndex < 0) {
                return 0;
            }
            Object value = row[tierKeyIndex];
            if (value == null) {
                throw new IOException("merge row is missing the tier key column");
            }
            return TierKeys.canonical(value);
        }
    }
}
