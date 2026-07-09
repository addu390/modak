package io.tierdb.lake.iceberg.commit;

import io.tierdb.common.DeltaBatch;
import io.tierdb.common.DeltaRowsBatch;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.PgValues;
import io.tierdb.common.RowBatchData;
import io.tierdb.lake.iceberg.IcebergPublish;
import io.tierdb.lake.iceberg.IcebergValues;
import io.tierdb.lake.commit.LakeCommitResult;
import io.tierdb.lake.commit.MergeWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Folds a {@link DeltaRowsBatch} into the Iceberg base as one {@code RowDelta},
 * adapting delta entries to the shared {@link IcebergRecordMerge} core.
 */
public final class IcebergMergeWriter implements MergeWriter {

    private final Table table;

    public IcebergMergeWriter(Table table) {
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
        IcebergRecordMerge.MergeFiles files;
        try (IcebergRecordMerge merge = new IcebergRecordMerge(
                table, rows.pkColumns(), /*participantId=*/ 1, System.nanoTime())) {
            boolean singlePk = merge.pkFields().size() == 1;
            int[] pkRowIndexes = pkRowIndexes(rows, merge.pkFields());
            for (DeltaRowsBatch.Entry e : rows.entries()) {
                if (e.tombstone()) {
                    merge.delete(keyRecord(merge, e, pkRowIndexes, singlePk), e.lakeTierKey());
                } else {
                    merge.upsert(rowRecord(merge.schema(), rows, e), e.tierKey(), e.lakeTierKey());
                }
            }
            files = merge.complete();
        }

        try {
            RowDelta rowDelta = table.newRowDelta();
            files.deleteFiles().forEach(rowDelta::addDeletes);
            files.dataFiles().forEach(rowDelta::addRows);
            snapshotProps.forEach(rowDelta::set);
            rowDelta.commit();
            table.refresh();
            Snapshot committed = table.currentSnapshot();
            return LakeCommitResult.committedIsReadable(
                    new LakeSnapshotId(committed.sequenceNumber()), IcebergPublish.props(table));
        } catch (RuntimeException e) {
            List<String> orphaned = new ArrayList<>();
            files.deleteFiles().forEach(f -> orphaned.add(f.path().toString()));
            files.dataFiles().forEach(f -> orphaned.add(f.path().toString()));
            CatalogUtil.deleteFiles(table.io(), orphaned, "orphaned merge file", true);
            throw new IOException("failed to fold delta into Iceberg table " + table.name(), e);
        }
    }

    private GenericRecord rowRecord(Schema schema, DeltaRowsBatch rows, DeltaRowsBatch.Entry e)
            throws IOException {
        GenericRecord record = GenericRecord.create(schema);
        for (int i = 0; i < rows.columns().size(); i++) {
            RowBatchData.Column col = rows.columns().get(i);
            Types.NestedField field = schema.findField(col.name());
            if (field == null) {
                throw new IOException("delta column '" + col.name()
                        + "' has no counterpart in the Iceberg schema of " + table.name());
            }
            record.setField(col.name(), IcebergValues.coerce(e.row()[i], field.type()));
        }
        return record;
    }

    private static GenericRecord keyRecord(IcebergRecordMerge merge, DeltaRowsBatch.Entry e,
            int[] pkRowIndexes, boolean singlePk) throws IOException {
        GenericRecord record = GenericRecord.create(merge.schema());
        List<Types.NestedField> pkFields = merge.pkFields();
        for (int i = 0; i < pkFields.size(); i++) {
            record.setField(pkFields.get(i).name(),
                    pkValue(e, pkFields.get(i), pkRowIndexes[i], singlePk));
        }
        return record;
    }

    private static int[] pkRowIndexes(DeltaRowsBatch rows, List<Types.NestedField> pkFields)
            throws IOException {
        int[] indexes = new int[pkFields.size()];
        for (int i = 0; i < pkFields.size(); i++) {
            indexes[i] = -1;
            for (int c = 0; c < rows.columns().size(); c++) {
                if (rows.columns().get(c).name().equals(pkFields.get(i).name())) {
                    indexes[i] = c;
                    break;
                }
            }
            if (indexes[i] < 0) {
                throw new IOException("PK column '" + pkFields.get(i).name()
                        + "' is not in the delta batch");
            }
        }
        return indexes;
    }

    private static Object pkValue(DeltaRowsBatch.Entry e, Types.NestedField field, int rowIndex,
            boolean singlePk) throws IOException {
        if (e.row() != null && e.row()[rowIndex] != null) {
            return IcebergValues.coerce(e.row()[rowIndex], field.type());
        }
        if (e.row() == null && e.tombstone() && singlePk) {
            return pkFromText(e.pk(), field.type());
        }
        throw new IOException("entry for pk '" + e.pk() + "' is missing key column '"
                + field.name() + "'");
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
