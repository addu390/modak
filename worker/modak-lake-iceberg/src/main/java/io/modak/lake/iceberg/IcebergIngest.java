package io.modak.lake.iceberg;

import io.modak.lake.ColdTableSpec;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.TierKeyWindow;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * The bulk ingest commit, staged Parquet files adopted by reference as one
 * atomic upsert. Overlapping tier-key ranges get equality deletes on the pk,
 * fresh ranges commit as a plain append with no read pass.
 */
final class IcebergIngest {

    private final Table table;
    private final ColdTableSpec spec;
    private final TruncatePartitioning partitioning;
    private final IcebergSnapshotCommit commit;

    IcebergIngest(Table table, ColdTableSpec spec) {
        this.table = table;
        this.spec = spec;
        this.partitioning = TruncatePartitioning.of(table);
        this.commit = new IcebergSnapshotCommit(table);
    }

    LakeCommitResult ingest(List<String> files, TierKeyWindow window,
            Map<String, String> snapshotProps) {
        if (files.isEmpty()) {
            return null;
        }
        table.refresh();
        List<Staged> staged = validate(files, window);
        return overlapsExistingData(staged)
                ? commitWithDeletes(staged, snapshotProps)
                : commit.apply(append(staged), snapshotProps);
    }

    private record Staged(String path, DataFile dataFile, long lo, long hi) {}

    private List<Staged> validate(List<String> files, TierKeyWindow window) {
        Types.NestedField field = requireField(spec.tierKeyCol());
        MetricsConfig metricsConfig = MetricsConfig.forTable(table);
        List<Staged> out = new ArrayList<>(files.size());
        for (String path : files) {
            InputFile in = table.io().newInputFile(path);
            Metrics metrics = ParquetUtil.fileMetrics(in, metricsConfig);
            long lo = bound(metrics.lowerBounds(), field, path);
            long hi = bound(metrics.upperBounds(), field, path);
            if (!window.containsRange(lo, hi)) {
                throw new IllegalArgumentException(path + " has tier keys [" + lo + ", " + hi
                        + "] outside the ingest window " + window);
            }
            DataFiles.Builder builder = DataFiles.builder(table.spec())
                    .withPath(in.location())
                    .withFileSizeInBytes(in.getLength())
                    .withFormat(FileFormat.PARQUET)
                    .withMetrics(metrics);
            if (partitioning.partitioned()) {
                builder.withPartitionPath(partitioning.singleBucketPath(lo, hi, path));
            }
            out.add(new Staged(path, builder.build(), lo, hi));
        }
        return out;
    }

    private boolean overlapsExistingData(List<Staged> staged) {
        long lo = staged.stream().mapToLong(Staged::lo).min().orElseThrow();
        long hi = staged.stream().mapToLong(Staged::hi).max().orElseThrow();
        return IcebergScans.anyFileMatches(table, Expressions.and(
                Expressions.greaterThanOrEqual(spec.tierKeyCol(), lo),
                Expressions.lessThanOrEqual(spec.tierKeyCol(), hi)));
    }

    private AppendFiles append(List<Staged> staged) {
        AppendFiles append = table.newAppend();
        staged.forEach(s -> append.appendFile(s.dataFile()));
        return append;
    }

    private LakeCommitResult commitWithDeletes(List<Staged> staged,
            Map<String, String> snapshotProps) {
        List<DeleteFile> deletes = writeEqualityDeletes(staged);
        try {
            RowDelta rowDelta = table.newRowDelta();
            deletes.forEach(rowDelta::addDeletes);
            staged.forEach(s -> rowDelta.addRows(s.dataFile()));
            return commit.apply(rowDelta, snapshotProps);
        } catch (RuntimeException e) {
            List<String> orphaned = new ArrayList<>();
            deletes.forEach(f -> orphaned.add(f.path().toString()));
            CatalogUtil.deleteFiles(table.io(), orphaned, "orphaned ingest delete file", true);
            throw e;
        }
    }

    private List<DeleteFile> writeEqualityDeletes(List<Staged> staged) {
        Schema schema = table.schema();
        List<String> pkCols = spec.pkCols();
        pkCols.forEach(this::requireField);
        int[] pkFieldIds = pkCols.stream()
                .mapToInt(c -> schema.findField(c).fieldId()).toArray();
        Schema deleteSchema = schema.select(pkCols);
        Schema readSchema = schema.select(projectedColumns());
        GenericAppenderFactory factory = new GenericAppenderFactory(
                schema, table.spec(), pkFieldIds, deleteSchema, null);
        OutputFileFactory outputs = OutputFileFactory.builderFor(
                        table, /*partitionId=*/ 1, System.nanoTime())
                .format(FileFormat.PARQUET)
                .build();

        PartitionFanout<EqualityDeleteWriter<Record>> writers = new PartitionFanout<>(
                partitioning,
                partition -> factory.newEqDeleteWriter(
                        partition == null
                                ? outputs.newOutputFile()
                                : outputs.newOutputFile(table.spec(), partition),
                        FileFormat.PARQUET, partition));
        try {
            for (Staged s : staged) {
                writeDeleteKeys(s.path(), readSchema, deleteSchema, writers);
            }
            for (EqualityDeleteWriter<Record> writer : writers.all()) {
                writer.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to derive equality deletes", e);
        }
        List<DeleteFile> out = new ArrayList<>();
        for (EqualityDeleteWriter<Record> writer : writers.all()) {
            out.add(writer.toDeleteFile());
        }
        return out;
    }

    private void writeDeleteKeys(String path, Schema readSchema, Schema deleteSchema,
            PartitionFanout<EqualityDeleteWriter<Record>> writers) throws IOException {
        try (CloseableIterable<Record> records = Parquet.read(table.io().newInputFile(path))
                .project(readSchema)
                .createReaderFunc(type -> GenericParquetReaders.buildReader(readSchema, type))
                .build()) {
            for (Record r : records) {
                GenericRecord key = GenericRecord.create(deleteSchema);
                for (String pkCol : spec.pkCols()) {
                    key.setField(pkCol, r.getField(pkCol));
                }
                long tierKey = ((Number) r.getField(spec.tierKeyCol())).longValue();
                writers.writerFor(tierKey).write(key);
            }
        }
    }

    private List<String> projectedColumns() {
        List<String> projected = new ArrayList<>(spec.pkCols());
        if (!projected.contains(spec.tierKeyCol())) {
            projected.add(spec.tierKeyCol());
        }
        return projected;
    }

    private Types.NestedField requireField(String name) {
        Types.NestedField field = table.schema().findField(name);
        if (field == null) {
            throw new IllegalArgumentException(
                    "column '" + name + "' not in the lake schema");
        }
        return field;
    }

    private static long bound(Map<Integer, ByteBuffer> bounds, Types.NestedField field,
            String path) {
        ByteBuffer buf = bounds == null ? null : bounds.get(field.fieldId());
        if (buf == null) {
            throw new IllegalArgumentException(
                    path + " has no statistics for tier-key column '" + field.name() + "'");
        }
        Object value = Conversions.fromByteBuffer(field.type(), buf);
        if (!(value instanceof Number n)) {
            throw new IllegalArgumentException("tier-key column '" + field.name()
                    + "' has non-numeric bounds in " + path);
        }
        return n.longValue();
    }
}
