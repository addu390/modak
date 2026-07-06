package io.tierdb.lake.iceberg;

import io.tierdb.common.DeltaBatch;
import io.tierdb.common.DeltaRowsBatch;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.PgValues;
import io.tierdb.common.RowBatchData;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.commit.MergeWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Folds a {@link DeltaRowsBatch} into the Iceberg base as one {@code RowDelta}:
 * equality deletes on the PK plus data files with the upsert images,
 * landing at one sequence number.
 */
final class IcebergMergeWriter implements MergeWriter {

    private final Table table;
    private boolean singlePk;

    IcebergMergeWriter(Table table) {
        this.table = table;
    }

    @Override
    public LakeCommitResult applyDelta(DeltaBatch batch, Map<String, String> snapshotProps)
            throws IOException {
        if (!(batch instanceof DeltaRowsBatch rows)) {
            throw new IOException("Iceberg merge writer expects DeltaRowsBatch, got "
                    + batch.getClass().getName());
        }
        table.refresh();
        Schema schema = table.schema();
        List<Types.NestedField> pkFields = new ArrayList<>(rows.pkColumns().size());
        for (String pkCol : rows.pkColumns()) {
            Types.NestedField field = schema.findField(pkCol);
            if (field == null) {
                throw new IOException("PK column '" + pkCol
                        + "' has no counterpart in the Iceberg schema of " + table.name());
            }
            pkFields.add(field);
        }
        this.singlePk = pkFields.size() == 1;

        OutputFileFactory files = OutputFileFactory.builderFor(
                        table, /*partitionId=*/ 1, System.nanoTime())
                .format(FileFormat.PARQUET)
                .build();

        List<DeleteFile> deletes = writeEqualityDeletes(rows, schema, pkFields, files);
        List<DataFile> upserts = writeUpserts(rows, schema, files);

        try {
            RowDelta rowDelta = table.newRowDelta();
            deletes.forEach(rowDelta::addDeletes);
            upserts.forEach(rowDelta::addRows);
            snapshotProps.forEach(rowDelta::set);
            rowDelta.commit();
            table.refresh();
            Snapshot committed = table.currentSnapshot();
            return LakeCommitResult.committedIsReadable(
                    new LakeSnapshotId(committed.sequenceNumber()), IcebergPublish.props(table));
        } catch (RuntimeException e) {
            List<String> orphaned = new ArrayList<>();
            deletes.forEach(f -> orphaned.add(f.path().toString()));
            upserts.forEach(f -> orphaned.add(f.path().toString()));
            CatalogUtil.deleteFiles(table.io(), orphaned, "orphaned merge file", true);
            throw new IOException("failed to fold delta into Iceberg table " + table.name(), e);
        }
    }

    private PartitionKey partitionOf(long tierKey, Schema schema) {
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

    private List<DeleteFile> writeEqualityDeletes(DeltaRowsBatch rows, Schema schema,
            List<Types.NestedField> pkFields, OutputFileFactory files) throws IOException {
        String[] pkNames = pkFields.stream().map(Types.NestedField::name).toArray(String[]::new);
        int[] pkFieldIds = pkFields.stream().mapToInt(Types.NestedField::fieldId).toArray();
        int[] pkRowIndexes = pkRowIndexes(rows, pkNames);
        Schema deleteSchema = schema.select(pkNames);
        GenericAppenderFactory factory = new GenericAppenderFactory(
                schema, table.spec(), pkFieldIds, deleteSchema, null);

        Map<Object, EqualityDeleteWriter<Record>> writers = new HashMap<>();
        try {
            for (DeltaRowsBatch.Entry e : rows.entries()) {
                GenericRecord key = GenericRecord.create(deleteSchema);
                for (int i = 0; i < pkFields.size(); i++) {
                    key.setField(pkNames[i], pkValue(e, pkFields.get(i), pkRowIndexes[i]));
                }
                PartitionKey partition = partitionOf(e.lakeTierKey(), schema);
                EqualityDeleteWriter<Record> writer =
                        writers.get(partition == null ? UNPARTITIONED : partition);
                if (writer == null) {
                    PartitionKey copy = partition == null ? null : partition.copy();
                    writer = factory.newEqDeleteWriter(
                            copy == null
                                    ? files.newOutputFile()
                                    : files.newOutputFile(table.spec(), copy),
                            FileFormat.PARQUET, copy);
                    writers.put(copy == null ? UNPARTITIONED : copy, writer);
                }
                writer.write(key);
            }
        } finally {
            for (EqualityDeleteWriter<Record> writer : writers.values()) {
                writer.close();
            }
        }
        List<DeleteFile> out = new ArrayList<>(writers.size());
        for (EqualityDeleteWriter<Record> writer : writers.values()) {
            out.add(writer.toDeleteFile());
        }
        return out;
    }

    private static final Object UNPARTITIONED = new Object();

    private int[] pkRowIndexes(DeltaRowsBatch rows, String[] pkNames) throws IOException {
        int[] indexes = new int[pkNames.length];
        for (int i = 0; i < pkNames.length; i++) {
            indexes[i] = -1;
            for (int c = 0; c < rows.columns().size(); c++) {
                if (rows.columns().get(c).name().equals(pkNames[i])) {
                    indexes[i] = c;
                    break;
                }
            }
            if (indexes[i] < 0) {
                throw new IOException("PK column '" + pkNames[i] + "' is not in the delta batch");
            }
        }
        return indexes;
    }

    private Object pkValue(DeltaRowsBatch.Entry e, Types.NestedField field, int rowIndex)
            throws IOException {
        if (e.row() != null && e.row()[rowIndex] != null) {
            return IcebergLakeWriter.coerce(e.row()[rowIndex], field.type());
        }
        if (e.row() == null && e.tombstone() && singlePk) {
            return pkFromText(e.pk(), field.type());
        }
        throw new IOException("entry for pk '" + e.pk() + "' is missing key column '"
                + field.name() + "'");
    }

    private List<DataFile> writeUpserts(DeltaRowsBatch rows, Schema schema,
            OutputFileFactory files) throws IOException {
        GenericAppenderFactory factory = new GenericAppenderFactory(schema, table.spec());
        Map<Object, DataWriter<Record>> writers = new HashMap<>();
        try {
            for (DeltaRowsBatch.Entry e : rows.entries()) {
                if (e.tombstone()) {
                    continue;
                }
                GenericRecord record = GenericRecord.create(schema);
                for (int i = 0; i < rows.columns().size(); i++) {
                    RowBatchData.Column col = rows.columns().get(i);
                    Types.NestedField field = schema.findField(col.name());
                    if (field == null) {
                        throw new IOException("delta column '" + col.name()
                                + "' has no counterpart in the Iceberg schema of " + table.name());
                    }
                    record.setField(col.name(), IcebergLakeWriter.coerce(e.row()[i], field.type()));
                }
                PartitionKey partition = partitionOf(e.tierKey(), schema);
                DataWriter<Record> writer =
                        writers.get(partition == null ? UNPARTITIONED : partition);
                if (writer == null) {
                    PartitionKey copy = partition == null ? null : partition.copy();
                    writer = factory.newDataWriter(
                            copy == null
                                    ? files.newOutputFile()
                                    : files.newOutputFile(table.spec(), copy),
                            FileFormat.PARQUET, copy);
                    writers.put(copy == null ? UNPARTITIONED : copy, writer);
                }
                writer.write(record);
            }
        } finally {
            for (DataWriter<Record> writer : writers.values()) {
                writer.close();
            }
        }
        List<DataFile> out = new ArrayList<>(writers.size());
        for (DataWriter<Record> writer : writers.values()) {
            out.add(writer.toDataFile());
        }
        return out;
    }

    private static Object pkFromText(String pk, Type type) throws IOException {
        return switch (type.typeId()) {
            case LONG -> Long.parseLong(pk);
            case INTEGER -> Integer.parseInt(pk);
            case STRING -> pk;
            case UUID -> java.util.UUID.fromString(pk);
            case DATE -> java.time.LocalDate.parse(pk);
            case DECIMAL -> new java.math.BigDecimal(pk);
            case TIMESTAMP -> PgValues.parseTimestamp(pk);
            default -> throw new IOException("unsupported PK type for the merge: " + type);
        };
    }
}
