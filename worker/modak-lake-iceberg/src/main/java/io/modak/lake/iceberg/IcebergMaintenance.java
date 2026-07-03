package io.modak.lake.iceberg;

import io.modak.lake.MaintenanceConfig;
import io.modak.lake.MaintenanceResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;

/**
 * Bin-packs small data files ({@code RewriteFiles}) and expires old snapshots.
 * Only delete-free files are rewritten, since a rewrite lands at a new sequence number
 * and older equality deletes would stop applying, resurrecting rows. Expiry never
 * touches snapshots at or above the pinned reader horizon.
 */
final class IcebergMaintenance {

    private final Table table;

    IcebergMaintenance(Table table) {
        this.table = table;
    }

    MaintenanceResult run(MaintenanceConfig config, long oldestPinnedSequence,
            Map<String, String> snapshotProps) throws IOException {
        table.refresh();
        if (table.currentSnapshot() == null) {
            return MaintenanceResult.NOOP;
        }
        int[] rewrite = rewriteSmallFiles(config, snapshotProps);
        int expired = expireSnapshots(config, oldestPinnedSequence);
        return new MaintenanceResult(rewrite[0], rewrite[1], expired);
    }

    private int[] rewriteSmallFiles(MaintenanceConfig config,
            Map<String, String> snapshotProps) throws IOException {
        Map<String, List<FileScanTask>> groups = new HashMap<>();
        int smallFiles = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.deletes().isEmpty()
                        || task.file().fileSizeInBytes() >= config.rewriteTargetBytes()) {
                    continue;
                }
                smallFiles++;
                groups.computeIfAbsent(
                        task.spec().specId() + "/" + task.file().partition(),
                        k -> new ArrayList<>()).add(task);
            }
        }
        if (smallFiles < config.rewriteMinInputFiles()) {
            return new int[] {0, 0};
        }

        OutputFileFactory files = OutputFileFactory.builderFor(table, 0, System.nanoTime())
                .format(FileFormat.PARQUET)
                .build();
        Set<DataFile> toDelete = new HashSet<>();
        Set<DataFile> toAdd = new HashSet<>();
        for (List<FileScanTask> group : groups.values()) {
            if (group.size() < 2) {
                continue;
            }
            toAdd.add(packGroup(group, files));
            group.forEach(t -> toDelete.add(t.file()));
        }
        if (toAdd.isEmpty()) {
            return new int[] {0, 0};
        }

        RewriteFiles rewrite = table.newRewrite().rewriteFiles(toDelete, toAdd);
        snapshotProps.forEach(rewrite::set);
        rewrite.commit();
        table.refresh();
        return new int[] {toDelete.size(), toAdd.size()};
    }

    private DataFile packGroup(List<FileScanTask> group,
            OutputFileFactory files) throws IOException {
        PartitionSpec spec = group.get(0).spec();
        StructLike partition = spec.isUnpartitioned() ? null : group.get(0).file().partition();
        GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), spec);
        DataWriter<Record> writer = factory.newDataWriter(
                partition == null
                        ? files.newOutputFile()
                        : files.newOutputFile(spec, partition),
                FileFormat.PARQUET, partition);
        try (writer) {
            for (FileScanTask task : group) {
                try (CloseableIterable<Record> rows =
                        Parquet.read(table.io().newInputFile(task.file().path().toString()))
                                .project(table.schema())
                                .createReaderFunc(fileSchema ->
                                        GenericParquetReaders.buildReader(table.schema(), fileSchema))
                                .build()) {
                    rows.forEach(writer::write);
                }
            }
        }
        return writer.toDataFile();
    }

    private int expireSnapshots(MaintenanceConfig config, long oldestPinnedSequence) {
        long ageBound = System.currentTimeMillis() - config.snapshotRetentionMillis();
        long pinBound = Long.MAX_VALUE;
        int before = 0;
        for (Snapshot s : table.snapshots()) {
            before++;
            if (s.sequenceNumber() >= oldestPinnedSequence) {
                pinBound = Math.min(pinBound, s.timestampMillis());
            }
        }
        // expireOlderThan is strictly-less-than: the pinned snapshot itself survives.
        table.expireSnapshots()
                .expireOlderThan(Math.min(ageBound, pinBound))
                .retainLast(config.snapshotMinRetained())
                .cleanExpiredFiles(true)
                .commit();
        table.refresh();
        int after = 0;
        for (Snapshot ignored : table.snapshots()) {
            after++;
        }
        return before - after;
    }
}
