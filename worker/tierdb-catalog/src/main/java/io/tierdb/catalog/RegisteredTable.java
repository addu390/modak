package io.tierdb.catalog;

import io.tierdb.common.TableId;
import io.tierdb.common.TierKeyType;
import java.util.List;
import java.util.Optional;

/**
 * A row of {@code tierdb.tables} as read back. {@code publicationName} and
 * {@code slotName} are set only for {@link TableMode#MIRRORED} tables, and the
 * retention lags are empty when the respective tier keeps everything.
 */
public record RegisteredTable(
        TableId id,
        String schemaName,
        String tableName,
        List<String> primaryKeyCols,
        String tierKeyCol,
        String partitionScheme,
        String lakeFormat,
        String lakeTableRef,
        String storageProfile,
        TableMode mode,
        String publicationName,
        String slotName,
        Optional<Long> heapRetentionLag,
        Optional<Long> lakeRetentionLag,
        boolean keepHeap,
        MaintenancePolicy maintenancePolicy,
        TierKeyType tierKeyType) {

    public boolean dropsHeapPartitions() {
        return (mode.tierSplitting() && !keepHeap) || heapRetentionLag.isPresent();
    }

    private static final java.util.regex.Pattern PARTITION_WIDTH =
            java.util.regex.Pattern.compile("\"partition_width\"\\s*:\\s*(\\d+)");

    private static final java.util.regex.Pattern LAKE_TRANSFORM =
            java.util.regex.Pattern.compile("\"lake_transform\"\\s*:\\s*\"([^\"]+)\"");

    public Optional<Long> partitionWidth() {
        if (partitionScheme == null) {
            return Optional.empty();
        }
        var m = PARTITION_WIDTH.matcher(partitionScheme);
        return m.find() ? Optional.of(Long.parseLong(m.group(1))) : Optional.empty();
    }

    public Optional<String> lakeTransform() {
        if (partitionScheme == null) {
            return Optional.empty();
        }
        var m = LAKE_TRANSFORM.matcher(partitionScheme);
        return m.find() ? Optional.of(m.group(1)) : Optional.empty();
    }
}
