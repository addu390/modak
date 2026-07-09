package io.tierdb.lake.iceberg.commit;

import io.tierdb.lake.iceberg.TierKeys;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;

/**
 * The record-level core of an Iceberg merge: equality deletes on the PK plus
 * data files with the upsert images, fanned out per partition by the canonical
 * tier key. The one implementation of "upsert into Iceberg", shared by the
 * worker's delta fold and the connectors' direct-mode writes; committing the
 * produced files is the caller's job.
 */
public final class IcebergRecordMerge implements Closeable {

    /** The files one participant wrote; serializable so engines can collect them. */
    public record MergeFiles(List<DataFile> dataFiles, List<DeleteFile> deleteFiles)
            implements Serializable {}

    private static final Object UNPARTITIONED = new Object();

    private final Table table;
    private final Schema schema;
    private final List<Types.NestedField> pkFields;
    private final Schema deleteSchema;
    private final OutputFileFactory files;
    private final GenericAppenderFactory deleteFactory;
    private final GenericAppenderFactory dataFactory;
    private final Map<Object, EqualityDeleteWriter<Record>> deleteWriters = new HashMap<>();
    private final Map<Object, DataWriter<Record>> dataWriters = new HashMap<>();
    private boolean closed;

    public IcebergRecordMerge(Table table, List<String> pkCols, int participantId, long taskId)
            throws IOException {
        this.table = table;
        this.schema = table.schema();
        this.pkFields = new ArrayList<>(pkCols.size());
        for (String pkCol : pkCols) {
            Types.NestedField field = schema.findField(pkCol);
            if (field == null) {
                throw new IOException("PK column '" + pkCol
                        + "' has no counterpart in the Iceberg schema of " + table.name());
            }
            pkFields.add(field);
        }
        String[] pkNames = pkFields.stream().map(Types.NestedField::name).toArray(String[]::new);
        int[] pkFieldIds = pkFields.stream().mapToInt(Types.NestedField::fieldId).toArray();
        this.deleteSchema = schema.select(pkNames);
        this.files = OutputFileFactory.builderFor(table, participantId, taskId)
                .format(FileFormat.PARQUET)
                .build();
        this.deleteFactory = new GenericAppenderFactory(
                schema, table.spec(), pkFieldIds, deleteSchema, null);
        this.dataFactory = new GenericAppenderFactory(schema, table.spec());
    }

    public Schema schema() {
        return schema;
    }

    public List<Types.NestedField> pkFields() {
        return pkFields;
    }

    /**
     * One upsert: an equality delete keyed on the PK in {@code deleteTierKey}'s
     * partition (the row's prior location) plus the new image in
     * {@code tierKey}'s.
     */
    public void upsert(Record row, long tierKey, long deleteTierKey) throws IOException {
        writeDelete(row, deleteTierKey);
        dataWriterFor(partitionOf(tierKey)).write(row);
    }

    /** One delete: an equality delete keyed on the PK fields of {@code key}. */
    public void delete(Record key, long tierKey) throws IOException {
        writeDelete(key, tierKey);
    }

    /** Closes every writer and returns the completed files. */
    public MergeFiles complete() throws IOException {
        closed = true;
        List<DeleteFile> deletes = new ArrayList<>(deleteWriters.size());
        for (EqualityDeleteWriter<Record> writer : deleteWriters.values()) {
            writer.close();
            deletes.add(writer.toDeleteFile());
        }
        List<DataFile> upserts = new ArrayList<>(dataWriters.size());
        for (DataWriter<Record> writer : dataWriters.values()) {
            writer.close();
            upserts.add(writer.toDataFile());
        }
        return new MergeFiles(upserts, deletes);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        for (EqualityDeleteWriter<Record> writer : deleteWriters.values()) {
            writer.close();
        }
        for (DataWriter<Record> writer : dataWriters.values()) {
            writer.close();
        }
    }

    private void writeDelete(Record source, long tierKey) throws IOException {
        GenericRecord key = GenericRecord.create(deleteSchema);
        for (Types.NestedField field : pkFields) {
            Object value = source.getField(field.name());
            if (value == null) {
                throw new IOException("merge entry is missing key column '" + field.name() + "'");
            }
            key.setField(field.name(), value);
        }
        deleteWriterFor(partitionOf(tierKey)).write(key);
    }

    private PartitionKey partitionOf(long tierKey) {
        PartitionSpec spec = table.spec();
        if (spec.isUnpartitioned()) {
            return null;
        }
        String tierKeyCol = schema.findColumnName(spec.fields().get(0).sourceId());
        GenericRecord probe = GenericRecord.create(schema);
        probe.setField(tierKeyCol, TierKeys.internalValue(
                schema.findField(tierKeyCol).type(), tierKey));
        PartitionKey key = new PartitionKey(spec, schema);
        key.partition(probe);
        return key;
    }

    private EqualityDeleteWriter<Record> deleteWriterFor(PartitionKey partition) {
        EqualityDeleteWriter<Record> writer =
                deleteWriters.get(partition == null ? UNPARTITIONED : partition);
        if (writer == null) {
            PartitionKey copy = partition == null ? null : partition.copy();
            writer = deleteFactory.newEqDeleteWriter(
                    copy == null
                            ? files.newOutputFile()
                            : files.newOutputFile(table.spec(), copy),
                    FileFormat.PARQUET, copy);
            deleteWriters.put(copy == null ? UNPARTITIONED : copy, writer);
        }
        return writer;
    }

    private DataWriter<Record> dataWriterFor(PartitionKey partition) {
        DataWriter<Record> writer =
                dataWriters.get(partition == null ? UNPARTITIONED : partition);
        if (writer == null) {
            PartitionKey copy = partition == null ? null : partition.copy();
            writer = dataFactory.newDataWriter(
                    copy == null
                            ? files.newOutputFile()
                            : files.newOutputFile(table.spec(), copy),
                    FileFormat.PARQUET, copy);
            dataWriters.put(copy == null ? UNPARTITIONED : copy, writer);
        }
        return writer;
    }
}
