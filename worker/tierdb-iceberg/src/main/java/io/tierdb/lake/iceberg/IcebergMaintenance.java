package io.tierdb.lake.iceberg;

import io.tierdb.lake.maintain.MaintenancePlan;
import io.tierdb.lake.maintain.MaintenanceResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RewriteManifests;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.parquet.Parquet;

/** Iceberg's interpretation of a {@link MaintenancePlan}: delete-debt compaction, snapshot expiry, and orphan file cleanup. */
final class IcebergMaintenance {

    /** Every setting this format understands, resolved against its defaults. */
    private record Knobs(
            boolean rewriteEnabled,
            long rewriteTargetBytes,
            int rewriteMinInputFiles,
            boolean snapshotExpiryEnabled,
            long snapshotRetentionMillis,
            int snapshotMinRetained,
            boolean deleteCompactionEnabled,
            int deleteCompactionMinDeletes,
            boolean manifestRewriteEnabled,
            int manifestRewriteMinManifests,
            boolean orphanSweepEnabled,
            long orphanGraceMillis) {

        static Knobs from(Map<String, String> settings) {
            return new Knobs(
                    boolOf(settings, "rewrite_enabled", true),
                    longOf(settings, "rewrite_target_bytes", 128L * 1024 * 1024),
                    (int) longOf(settings, "rewrite_min_input_files", 8),
                    boolOf(settings, "snapshot_expiry_enabled", true),
                    longOf(settings, "snapshot_retention_hours", 24) * 3_600_000L,
                    (int) longOf(settings, "snapshot_min_retained", 5),
                    boolOf(settings, "delete_compaction_enabled", true),
                    (int) longOf(settings, "delete_compaction_min_deletes", 1),
                    boolOf(settings, "manifest_rewrite_enabled", true),
                    (int) longOf(settings, "manifest_rewrite_min_manifests", 100),
                    boolOf(settings, "orphan_sweep_enabled", false),
                    longOf(settings, "orphan_grace_hours", 72) * 3_600_000L);
        }

        private static long longOf(Map<String, String> settings, String key, long fallback) {
            String v = settings.get(key);
            return v == null ? fallback : Long.parseLong(v);
        }

        private static boolean boolOf(Map<String, String> settings, String key, boolean fallback) {
            String v = settings.get(key);
            return v == null ? fallback : Boolean.parseBoolean(v);
        }
    }

    private final Table table;

    IcebergMaintenance(Table table) {
        this.table = table;
    }

    MaintenanceResult run(MaintenancePlan plan, Map<String, String> snapshotProps)
            throws IOException {
        table.refresh();
        if (table.currentSnapshot() == null) {
            return MaintenanceResult.NOOP;
        }
        Knobs knobs = Knobs.from(plan.settings());
        Map<String, Long> counters = new LinkedHashMap<>();
        if (knobs.deleteCompactionEnabled()) {
            compactDeleteDebt(knobs, snapshotProps, counters);
        }
        if (knobs.rewriteEnabled()) {
            rewriteSmallFiles(knobs, snapshotProps, counters);
        }
        if (knobs.manifestRewriteEnabled()) {
            rewriteManifests(knobs, snapshotProps, counters);
        }
        if (knobs.snapshotExpiryEnabled()) {
            counters.put("expired_snapshots",
                    (long) expireSnapshots(knobs, plan.pinnedSnapshotFloor()));
        }
        sweepOrphans(knobs, plan, counters);
        return new MaintenanceResult(counters);
    }

    private void compactDeleteDebt(Knobs knobs, Map<String, String> snapshotProps,
            Map<String, Long> counters) throws IOException {
        Map<String, FileScanTask> carriers = new LinkedHashMap<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.deletes().isEmpty()) {
                    carriers.putIfAbsent(task.file().path().toString(), task);
                }
            }
        }
        Map<String, FileScanTask> selected = new LinkedHashMap<>();
        carriers.forEach((path, task) -> {
            if (task.deletes().size() >= knobs.deleteCompactionMinDeletes()) {
                selected.put(path, task);
            }
        });
        if (selected.isEmpty()) {
            counters.put("delete_compacted_files", 0L);
            counters.put("removed_delete_files", 0L);
            return;
        }

        Map<String, DeleteFile> removableDeletes = new HashMap<>();
        for (FileScanTask task : selected.values()) {
            for (DeleteFile d : task.deletes()) {
                removableDeletes.putIfAbsent(d.path().toString(), d);
            }
        }
        for (FileScanTask task : carriers.values()) {
            if (!selected.containsKey(task.file().path().toString())) {
                task.deletes().forEach(d -> removableDeletes.remove(d.path().toString()));
            }
        }

        OutputFileFactory files = OutputFileFactory.builderFor(table, 0, System.nanoTime())
                .format(FileFormat.PARQUET)
                .build();
        Map<String, List<FileScanTask>> groups = new LinkedHashMap<>();
        for (FileScanTask task : selected.values()) {
            groups.computeIfAbsent(
                    task.spec().specId() + "/" + task.file().partition(),
                    k -> new ArrayList<>()).add(task);
        }
        Set<DataFile> toAdd = new HashSet<>();
        for (List<FileScanTask> group : groups.values()) {
            DataFile packed = packGroup(group, files, /*applyDeletes=*/ true);
            if (packed.recordCount() > 0) {
                toAdd.add(packed);
            } else {
                table.io().deleteFile(packed.path().toString());
            }
        }
        Set<DataFile> toDelete = new HashSet<>();
        selected.values().forEach(t -> toDelete.add(t.file()));

        RewriteFiles rewrite = table.newRewrite()
                .validateFromSnapshot(table.currentSnapshot().snapshotId())
                .rewriteFiles(toDelete, Set.copyOf(removableDeletes.values()), toAdd, Set.of());
        snapshotProps.forEach(rewrite::set);
        rewrite.commit();
        table.refresh();
        counters.put("delete_compacted_files", (long) toDelete.size());
        counters.put("removed_delete_files", (long) removableDeletes.size());
    }

    private void rewriteSmallFiles(Knobs knobs, Map<String, String> snapshotProps,
            Map<String, Long> counters) throws IOException {
        counters.put("rewritten_files", 0L);
        counters.put("added_files", 0L);
        Map<String, List<FileScanTask>> groups = new HashMap<>();
        int smallFiles = 0;
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (!task.deletes().isEmpty()
                        || task.file().fileSizeInBytes() >= knobs.rewriteTargetBytes()) {
                    continue;
                }
                smallFiles++;
                groups.computeIfAbsent(
                        task.spec().specId() + "/" + task.file().partition(),
                        k -> new ArrayList<>()).add(task);
            }
        }
        if (smallFiles < knobs.rewriteMinInputFiles()) {
            return;
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
            toAdd.add(packGroup(group, files, /*applyDeletes=*/ false));
            group.forEach(t -> toDelete.add(t.file()));
        }
        if (toAdd.isEmpty()) {
            return;
        }

        RewriteFiles rewrite = table.newRewrite().rewriteFiles(toDelete, toAdd);
        snapshotProps.forEach(rewrite::set);
        rewrite.commit();
        table.refresh();
        counters.put("rewritten_files", (long) toDelete.size());
        counters.put("added_files", (long) toAdd.size());
    }

    private DataFile packGroup(List<FileScanTask> group, OutputFileFactory files,
            boolean applyDeletes) throws IOException {
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
                try (CloseableIterable<Record> rows = openRows(task, applyDeletes)) {
                    rows.forEach(writer::write);
                }
            }
        }
        return writer.toDataFile();
    }

    private CloseableIterable<Record> openRows(FileScanTask task, boolean applyDeletes) {
        if (!applyDeletes || task.deletes().isEmpty()) {
            return readParquet(task, table.schema());
        }
        GenericDeleteFilter filter = new GenericDeleteFilter(
                table.io(), task, table.schema(), table.schema());
        return filter.filter(readParquet(task, filter.requiredSchema()));
    }

    private CloseableIterable<Record> readParquet(FileScanTask task,
            org.apache.iceberg.Schema projection) {
        return Parquet.read(table.io().newInputFile(task.file().path().toString()))
                .project(projection)
                .createReaderFunc(fileSchema ->
                        GenericParquetReaders.buildReader(projection, fileSchema))
                .build();
    }

    private void rewriteManifests(Knobs knobs, Map<String, String> snapshotProps,
            Map<String, Long> counters) {
        int manifests = table.currentSnapshot().allManifests(table.io()).size();
        if (manifests < knobs.manifestRewriteMinManifests()) {
            counters.put("rewritten_manifests", 0L);
            return;
        }
        RewriteManifests rewrite = table.rewriteManifests()
                .clusterBy(file -> String.valueOf(file.partition()));
        snapshotProps.forEach(rewrite::set);
        rewrite.commit();
        table.refresh();
        counters.put("rewritten_manifests", (long) manifests);
    }

    private int expireSnapshots(Knobs knobs, long pinnedSnapshotFloor) {
        long ageBound = System.currentTimeMillis() - knobs.snapshotRetentionMillis();
        long pinBound = Long.MAX_VALUE;
        int before = 0;
        for (Snapshot s : table.snapshots()) {
            before++;
            if (s.sequenceNumber() >= pinnedSnapshotFloor) {
                pinBound = Math.min(pinBound, s.timestampMillis());
            }
        }
        table.expireSnapshots()
                .expireOlderThan(Math.min(ageBound, pinBound))
                .retainLast(knobs.snapshotMinRetained())
                .cleanExpiredFiles(true)
                .commit();
        table.refresh();
        int after = 0;
        for (Snapshot ignored : table.snapshots()) {
            after++;
        }
        return before - after;
    }

    private void sweepOrphans(Knobs knobs, MaintenancePlan plan, Map<String, Long> counters) {
        if (!knobs.orphanSweepEnabled()
                || !(table.io() instanceof SupportsPrefixOperations listable)) {
            return;
        }
        long cutoff = System.currentTimeMillis() - knobs.orphanGraceMillis();
        Set<String> referenced = new HashSet<>();
        referencedFiles().forEach(path -> referenced.add(normalize(path)));
        Set<String> protectedFiles = new HashSet<>();
        plan.protectedFiles().forEach(path -> protectedFiles.add(normalize(path)));
        List<String> orphans = new ArrayList<>();
        for (FileInfo file : listable.listPrefix(table.location() + "/data")) {
            String path = normalize(file.location());
            if (file.createdAtMillis() < cutoff
                    && !referenced.contains(path)
                    && !protectedFiles.contains(path)) {
                orphans.add(file.location());
            }
        }
        orphans.forEach(path -> table.io().deleteFile(path));
        counters.put("orphan_files_deleted", (long) orphans.size());
    }

    private static String normalize(String location) {
        if (!location.startsWith("file:")) {
            return location;
        }
        String path = location.substring("file:".length());
        int i = 0;
        while (i + 1 < path.length() && path.charAt(i) == '/' && path.charAt(i + 1) == '/') {
            i++;
        }
        return path.substring(i);
    }

    private Set<String> referencedFiles() {
        Set<String> out = new HashSet<>();
        Set<String> seenManifests = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            for (ManifestFile manifest : snapshot.allManifests(table.io())) {
                if (!seenManifests.add(manifest.path())) {
                    continue;
                }
                if (manifest.content() == ManifestContent.DATA) {
                    try (ManifestReader<DataFile> reader =
                            ManifestFiles.read(manifest, table.io())) {
                        reader.forEach(f -> out.add(f.path().toString()));
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                } else {
                    try (ManifestReader<DeleteFile> reader = ManifestFiles.readDeleteManifest(
                            manifest, table.io(), table.specs())) {
                        reader.forEach(f -> out.add(f.path().toString()));
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
            }
        }
        return out;
    }
}
