package io.modak.lake.iceberg;

import io.modak.lake.LakeStats;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Iceberg's {@link LakeStats}: current snapshot summary counters (no file
 * listing) plus the health warnings only this format can judge, like
 * delete-file debt and manifest sprawl.
 */
final class IcebergStats {

    static final double DELETE_RATIO_WARN = 0.3;
    static final int DELETE_RATIO_MIN_DATA_FILES = 8;
    static final int MANIFESTS_WARN = 100;

    private final Table table;

    IcebergStats(Table table) {
        this.table = table;
    }

    LakeStats collect() {
        table.refresh();
        Snapshot current = table.currentSnapshot();
        if (current == null) {
            return LakeStats.EMPTY;
        }
        long dataFiles = summaryLong(current, "total-data-files");
        long deleteFiles = summaryLong(current, "total-delete-files");
        int snapshots = 0;
        for (Snapshot ignored : table.snapshots()) {
            snapshots++;
        }
        int manifests = current.allManifests(table.io()).size();

        Map<String, Double> values = new LinkedHashMap<>();
        values.put(LakeStats.FILES, (double) dataFiles);
        values.put("delete_files", (double) deleteFiles);
        values.put(LakeStats.BYTES, (double) summaryLong(current, "total-files-size"));
        values.put("records", (double) summaryLong(current, "total-records"));
        values.put(LakeStats.SNAPSHOTS, (double) snapshots);
        values.put("manifests", (double) manifests);
        double deleteRatio = dataFiles == 0 ? 0 : (double) deleteFiles / dataFiles;
        values.put("delete_ratio", deleteRatio);

        List<String> warnings = new ArrayList<>();
        if (dataFiles >= DELETE_RATIO_MIN_DATA_FILES && deleteRatio >= DELETE_RATIO_WARN) {
            warnings.add(String.format(
                    "delete files are %.0f%% of data files, delete debt is slowing reads",
                    deleteRatio * 100));
        }
        if (manifests >= MANIFESTS_WARN) {
            warnings.add(manifests + " manifests in the current snapshot, "
                    + "metadata is due for a rewrite");
        }
        return new LakeStats(values, warnings);
    }

    private static long summaryLong(Snapshot snapshot, String key) {
        String value = snapshot.summary().get(key);
        return value == null ? 0 : Long.parseLong(value);
    }
}
