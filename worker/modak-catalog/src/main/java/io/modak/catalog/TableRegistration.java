package io.modak.catalog;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Request to register a logical table in {@code modak.tables}. Format-agnostic:
 * {@code lakeFormat} names the plugin, {@code lakeTableRef} its table identifier,
 * {@code lakeProps} opaque per-format config. {@code publicationName} and
 * {@code slotName} are required for {@link TableMode#MIRRORED}.
 */
public record TableRegistration(
        long oid,
        String schemaName,
        String tableName,
        List<String> primaryKeyCols,
        String tierKeyCol,
        String partitionScheme,
        String lakeFormat,
        String lakeTableRef,
        String lakeProps,
        TableMode mode,
        String publicationName,
        String slotName,
        Optional<Long> heapRetentionLag,
        Optional<Long> lakeRetentionLag) {

    public TableRegistration {
        Objects.requireNonNull(schemaName);
        Objects.requireNonNull(tableName);
        primaryKeyCols = List.copyOf(primaryKeyCols);
        Objects.requireNonNull(tierKeyCol);
        Objects.requireNonNull(partitionScheme);
        Objects.requireNonNull(lakeFormat);
        Objects.requireNonNull(lakeTableRef);
        Objects.requireNonNull(mode);
        Objects.requireNonNull(heapRetentionLag);
        Objects.requireNonNull(lakeRetentionLag);
        if (primaryKeyCols.isEmpty()) {
            throw new IllegalArgumentException("primaryKeyCols must be non-empty (the merge key)");
        }
        if (mode == TableMode.MIRRORED
                && (publicationName == null || slotName == null)) {
            throw new IllegalArgumentException(
                    "mirrored registration needs a publication and a replication slot");
        }
        if (mode == TableMode.TIERED && heapRetentionLag.isPresent()) {
            throw new IllegalArgumentException(
                    "heapRetentionLag applies only to mirrored tables (tiered eviction is the cut-line)");
        }
        if (mode == TableMode.MIRRORED && lakeRetentionLag.isPresent()) {
            throw new IllegalArgumentException(
                    "lakeRetentionLag applies only to tiered tables (a mirrored heap drop "
                            + "relies on the lake holding full history)");
        }
        if (lakeRetentionLag.isPresent() && lakeRetentionLag.get() < 0) {
            throw new IllegalArgumentException(
                    "lakeRetentionLag must be >= 0: " + lakeRetentionLag.get());
        }
    }

    /** Tiered-mode registration — the shape that existed before table modes. */
    public TableRegistration(
            long oid,
            String schemaName,
            String tableName,
            List<String> primaryKeyCols,
            String tierKeyCol,
            String partitionScheme,
            String lakeFormat,
            String lakeTableRef,
            String lakeProps) {
        this(oid, schemaName, tableName, primaryKeyCols, tierKeyCol, partitionScheme,
                lakeFormat, lakeTableRef, lakeProps,
                TableMode.TIERED, null, null, Optional.empty(), Optional.empty());
    }
}
