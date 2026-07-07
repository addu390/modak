package io.tierdb.lake.iceberg;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;

/**
 * The publishable lake_props of a table's current version: metadata_location and
 * snapshot_id, read from one TableMetadata so the pair is always consistent.
 */
public final class IcebergPublish {

    private IcebergPublish() {}

    public static Map<String, String> props(Table table) {
        Map<String, String> props = new HashMap<>();
        var metadata = ((BaseTable) table).operations().current();
        props.put("metadata_location", metadata.metadataFileLocation());
        if (metadata.currentSnapshot() != null) {
            props.put("snapshot_id", Long.toString(metadata.currentSnapshot().snapshotId()));
        }
        return props;
    }
}
