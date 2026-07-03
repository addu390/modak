package io.modak.catalog;

import io.modak.common.TableId;
import java.util.List;
import java.util.Optional;

/**
 * A row of {@code modak.tables} as read back. {@code publicationName} and
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
        String lakeProps,
        int schemaVersion,
        TableMode mode,
        String publicationName,
        String slotName,
        Optional<Long> heapRetentionLag,
        Optional<Long> lakeRetentionLag) {

    /** Tiered-mode row, the shape that existed before table modes. */
    public RegisteredTable(
            TableId id,
            String schemaName,
            String tableName,
            List<String> primaryKeyCols,
            String tierKeyCol,
            String partitionScheme,
            String lakeFormat,
            String lakeTableRef,
            String lakeProps,
            int schemaVersion) {
        this(id, schemaName, tableName, primaryKeyCols, tierKeyCol, partitionScheme,
                lakeFormat, lakeTableRef, lakeProps, schemaVersion,
                TableMode.TIERED, null, null, Optional.empty(), Optional.empty());
    }

    /** Mirrored table that also drops heap partitions below the retention line. */
    public boolean dropsHeapPartitions() {
        return mode == TableMode.TIERED || heapRetentionLag.isPresent();
    }

    private static final java.util.regex.Pattern PARTITION_WIDTH =
            java.util.regex.Pattern.compile("\"partition_width\"\\s*:\\s*(\\d+)");

    /** The width declared in {@code partition_scheme}, when the scheme has one. */
    public Optional<Long> partitionWidth() {
        if (partitionScheme == null) {
            return Optional.empty();
        }
        var m = PARTITION_WIDTH.matcher(partitionScheme);
        return m.find() ? Optional.of(Long.parseLong(m.group(1))) : Optional.empty();
    }
}
