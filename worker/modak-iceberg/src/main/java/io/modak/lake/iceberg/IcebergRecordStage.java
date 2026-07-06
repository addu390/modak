package io.modak.lake.iceberg;

import io.modak.common.PgValues;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.FileFormat;
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
 * Writes raw rows into the lake table's own storage as staged Parquet files,
 * grouped by partition so no file straddles a boundary. Commits nothing, the
 * ingest commit adopts the paths.
 */
final class IcebergRecordStage {

    private final Table table;
    private final TierKeyPartitioning partitioning;

    IcebergRecordStage(Table table) {
        this.table = table;
        this.partitioning = TierKeyPartitioning.of(table);
    }

    List<String> stage(List<String> columns, Iterable<Object[]> rows) {
        Schema schema = table.schema();
        Types.NestedField[] fields = new Types.NestedField[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            fields[i] = schema.findField(columns.get(i));
            if (fields[i] == null) {
                throw new IllegalArgumentException(
                        "column '" + columns.get(i) + "' not in the lake schema");
            }
        }

        GenericAppenderFactory factory = new GenericAppenderFactory(schema, table.spec());
        OutputFileFactory outputs = OutputFileFactory.builderFor(
                        table, /*partitionId=*/ 1, System.nanoTime())
                .format(FileFormat.PARQUET)
                .build();
        PartitionFanout<DataWriter<Record>> writers = new PartitionFanout<>(
                partitioning,
                partition -> factory.newDataWriter(
                        partition == null
                                ? outputs.newOutputFile()
                                : outputs.newOutputFile(table.spec(), partition),
                        FileFormat.PARQUET, partition));

        try {
            for (Object[] row : rows) {
                if (row.length != columns.size()) {
                    throw new IllegalArgumentException("row has " + row.length
                            + " values for " + columns.size() + " columns");
                }
                GenericRecord record = GenericRecord.create(schema);
                for (int i = 0; i < columns.size(); i++) {
                    record.setField(columns.get(i), convert(row[i], fields[i]));
                }
                writers.writerFor(tierKeyOf(record)).write(record);
            }
            for (DataWriter<Record> writer : writers.all()) {
                writer.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to stage rows", e);
        }
        List<String> out = new ArrayList<>();
        for (DataWriter<Record> writer : writers.all()) {
            out.add(writer.toDataFile().path().toString());
        }
        return out;
    }

    private long tierKeyOf(Record record) {
        if (!partitioning.partitioned()) {
            return 0;
        }
        return TierKeys.canonical(record.getField(partitioning.sourceColumn()));
    }

    private static Object convert(Object v, Types.NestedField field) {
        if (v == null) {
            if (field.isRequired()) {
                throw new IllegalArgumentException(
                        "required column '" + field.name() + "' is null");
            }
            return null;
        }
        Type type = field.type();
        try {
            return switch (type.typeId()) {
                case LONG -> ((Number) v).longValue();
                case INTEGER -> ((Number) v).intValue();
                case DOUBLE -> ((Number) v).doubleValue();
                case FLOAT -> ((Number) v).floatValue();
                case BOOLEAN -> v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
                case STRING -> v.toString();
                case DECIMAL -> new BigDecimal(v.toString()).setScale(
                        ((Types.DecimalType) type).scale(), java.math.RoundingMode.HALF_UP);
                case UUID -> java.util.UUID.fromString(v.toString());
                case DATE -> java.time.LocalDate.parse(v.toString());
                case TIMESTAMP -> timestamp(v, (Types.TimestampType) type);
                default -> throw new IllegalArgumentException(
                        "unsupported staging type: " + type);
            };
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("cannot convert '" + v + "' to " + type
                    + " for column '" + field.name() + "'", e);
        }
    }

    private static Object timestamp(Object v, Types.TimestampType type) {
        OffsetDateTime odt = v instanceof OffsetDateTime o ? o
                : v instanceof LocalDateTime l
                        ? l.atOffset(ZoneOffset.UTC)
                        : PgValues.parseTimestamp(v.toString());
        return type.shouldAdjustToUTC() ? odt : odt.toLocalDateTime();
    }
}
