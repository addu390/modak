package io.modak.lake.iceberg;

import io.modak.common.PartitionData;
import io.modak.common.RowBatchData;
import io.modak.lake.LakeWriter;
import io.modak.lake.WriterInitContext;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Writes one hot partition's {@link RowBatchData} as Parquet data files into the
 * Iceberg table, fanning records out per Iceberg partition when the table has a
 * spec (tier-key truncate). Produces {@link DataFile} handles only, nothing is
 * visible until {@link IcebergLakeCommitter} commits the whole op as one snapshot.
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
        // Partition-scoped ids plus UUID-salted filenames: crash re-runs cannot collide.
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
        // Bound lazily so new heap columns can evolve the lake schema first.
        if (factory == null) {
            new IcebergSchemaEvolution(table).addMissing(batch.columns());
            factory = new GenericAppenderFactory(table.schema(), table.spec());
        }
        Schema schema = table.schema();
        PartitionSpec spec = table.spec();
        PartitionKey key = spec.isUnpartitioned() ? null : new PartitionKey(spec, schema);
        for (Object[] row : batch.rows()) {
            GenericRecord record = GenericRecord.create(schema);
            for (int i = 0; i < batch.columns().size(); i++) {
                RowBatchData.Column col = batch.columns().get(i);
                Types.NestedField field = schema.findField(col.name());
                if (field == null) {
                    throw new IOException("hot column '" + col.name()
                            + "' has no counterpart in the Iceberg schema of " + table.name());
                }
                record.setField(col.name(), coerce(row[i], field.type()));
            }
            if (key != null) {
                key.partition(record);
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

    // An empty partition still tiers as one empty file: journal and snapshot stay 1:1.
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
        probe.setField(tierKeyCol, ctx.bounds().lo().value());
        PartitionKey key = new PartitionKey(spec, schema);
        key.partition(probe);
        writerFor(key);
    }

    @Override
    public void close() throws IOException {
        for (DataWriter<Record> writer : writers.values()) {
            writer.close(); // idempotent, abandoned files are orphans until commit
        }
    }

    static Object coerce(Object v, Type type) {
        if (v == null) {
            return null;
        }
        if (type.typeId() == Type.TypeID.INTEGER && v instanceof Long l) {
            return Math.toIntExact(l);
        }
        if (type.typeId() == Type.TypeID.FLOAT && v instanceof Double d) {
            return (float) (double) d;
        }
        // Parquet requires the value's scale to match the column's declared scale.
        if (type instanceof Types.DecimalType dec && v instanceof BigDecimal bd) {
            return bd.setScale(dec.scale(), RoundingMode.HALF_UP);
        }
        if (type.typeId() == Type.TypeID.BINARY && v instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes);
        }
        if (type instanceof Types.TimestampType ts && v instanceof OffsetDateTime odt
                && !ts.shouldAdjustToUTC()) {
            return odt.toLocalDateTime();
        }
        return v;
    }
}
