package io.tierdb.lake.iceberg.commit;

import io.tierdb.common.PartitionData;
import io.tierdb.common.RowBatchData;
import io.tierdb.lake.iceberg.IcebergSchemaEvolution;
import io.tierdb.lake.iceberg.IcebergValues;
import io.tierdb.lake.iceberg.TierKeys;
import io.tierdb.lake.commit.LakeWriter;
import io.tierdb.lake.commit.WriterInitContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Types;

/**
 * Writes one hot partition's {@link RowBatchData} as Parquet data files
 * into the Iceberg table, fanning records out per Iceberg partition when
 * the table has a spec (tier-key truncate).
 */
final class IcebergLakeWriter implements LakeWriter<IcebergWriteResult> {

    private static final Object UNPARTITIONED = new Object();

    private final Table table;
    private final WriterInitContext ctx;
    private final OutputFileFactory files;
    private final Map<Object, DataWriter<Record>> writers = new HashMap<>();
    private GenericAppenderFactory factory;
    private final List<DataFile> completed = new ArrayList<>();

    IcebergLakeWriter(Table table, WriterInitContext ctx) throws IOException {
        this.table = table;
        this.ctx = ctx;
        this.files = OutputFileFactory.builderFor(
                        table, Math.abs(ctx.partition().id().hashCode()), System.nanoTime())
                .format(FileFormat.PARQUET)
                .build();
    }

    @Override
    public void write(PartitionData data) throws IOException {
        if (!(data instanceof RowBatchData batch)) {
            throw new IOException("Iceberg writer expects RowBatchData, got "
                    + data.getClass().getName());
        }
        if (factory == null) {
            new IcebergSchemaEvolution(table).addMissing(batch.columns());
            factory = new GenericAppenderFactory(table.schema(), table.spec());
        }

        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        PartitionKey key = spec.isUnpartitioned() ? null : new PartitionKey(spec, schema);
        InternalRecordWrapper wrapper = key == null
                ? null
                : new InternalRecordWrapper(schema.asStruct());

        for (Object[] row : batch.rows()) {
            GenericRecord record = GenericRecord.create(schema);
            for (int i = 0; i < batch.columns().size(); i++) {
                RowBatchData.Column col = batch.columns().get(i);
                Types.NestedField field = schema.findField(col.name());
                if (field == null) {
                    throw new IOException("hot column '" + col.name()
                            + "' has no counterpart in the Iceberg schema of " + table.name());
                }
                record.setField(col.name(), IcebergValues.coerce(row[i], field.type()));
            }

            if (key != null) {
                key.partition(wrapper.wrap(record));
            }
            writerFor(key).write(record);
        }
    }

    private DataWriter<Record> writerFor(PartitionKey key) {
        DataWriter<Record> writer = writers.get(key == null ? UNPARTITIONED : key);
        if (writer == null) {
            PartitionKey partition = key == null ? null : key.copy();
            writer = factory.newDataWriter(
                    partition == null
                            ? files.newOutputFile()
                            : files.newOutputFile(table.spec(), partition),
                    FileFormat.PARQUET, partition);
            writers.put(partition == null ? UNPARTITIONED : partition, writer);
        }
        return writer;
    }

    @Override
    public IcebergWriteResult complete() throws IOException {
        if (writers.isEmpty()) {
            emptyFile();
        }
        for (DataWriter<Record> writer : writers.values()) {
            writer.close();
            completed.add(writer.toDataFile());
        }
        return new IcebergWriteResult(completed);
    }

    private void emptyFile() {
        if (factory == null) {
            factory = new GenericAppenderFactory(table.schema(), table.spec());
        }
        PartitionSpec spec = table.spec();
        if (spec.isUnpartitioned()) {
            writerFor(null);
            return;
        }
        Schema schema = table.schema();
        String tierKeyCol = schema.findColumnName(spec.fields().get(0).sourceId());
        GenericRecord probe = GenericRecord.create(schema);
        probe.setField(tierKeyCol, TierKeys.internalValue(
                schema.findField(tierKeyCol).type(), ctx.bounds().lo().value()));
        PartitionKey key = new PartitionKey(spec, schema);
        key.partition(probe);
        writerFor(key);
    }

    @Override
    public void close() throws IOException {
        for (DataWriter<Record> writer : writers.values()) {
            writer.close();
        }
    }
}
