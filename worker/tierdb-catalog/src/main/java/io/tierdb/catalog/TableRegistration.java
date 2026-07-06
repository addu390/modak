package io.tierdb.catalog;

import io.tierdb.common.TierKeyType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Request to register a logical table in {@code tierdb.tables}. Format-agnostic:
 * {@code lakeFormat} names the plugin, {@code lakeTableRef} its table identifier.
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
        TableMode mode,
        String publicationName,
        String slotName,
        Optional<Long> heapRetentionLag,
        Optional<Long> lakeRetentionLag,
        boolean keepHeap,
        String storageProfile,
        TierKeyType tierKeyType) {

    public TableRegistration {
        Objects.requireNonNull(schemaName);
        Objects.requireNonNull(tableName);
        primaryKeyCols = List.copyOf(primaryKeyCols);
        Objects.requireNonNull(tierKeyCol);
        Objects.requireNonNull(tierKeyType);
        Objects.requireNonNull(partitionScheme);
        Objects.requireNonNull(lakeFormat);
        Objects.requireNonNull(lakeTableRef);
        Objects.requireNonNull(mode);
        Objects.requireNonNull(storageProfile);
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
        if (keepHeap && mode != TableMode.TIERED) {
            throw new IllegalArgumentException(
                    "keepHeap applies only to tiered tables (a mirrored heap is already kept)");
        }
        if (keepHeap && lakeRetentionLag.isPresent()) {
            throw new IllegalArgumentException(
                    "keepHeap and lakeRetentionLag exclude each other (keep-heap deletes nothing)");
        }
    }

    public static final String DEFAULT_PROFILE = "default";

    public TableRegistration(
            long oid,
            String schemaName,
            String tableName,
            List<String> primaryKeyCols,
            String tierKeyCol,
            String partitionScheme,
            String lakeFormat,
            String lakeTableRef,
            TableMode mode,
            String publicationName,
            String slotName,
            Optional<Long> heapRetentionLag,
            Optional<Long> lakeRetentionLag,
            boolean keepHeap,
            String storageProfile) {
        this(oid, schemaName, tableName, primaryKeyCols, tierKeyCol, partitionScheme,
                lakeFormat, lakeTableRef, mode, publicationName, slotName,
                heapRetentionLag, lakeRetentionLag, keepHeap, storageProfile,
                TierKeyType.BIGINT);
    }

    public TableRegistration(
            long oid,
            String schemaName,
            String tableName,
            List<String> primaryKeyCols,
            String tierKeyCol,
            String partitionScheme,
            String lakeFormat,
            String lakeTableRef,
            TableMode mode,
            String publicationName,
            String slotName,
            Optional<Long> heapRetentionLag,
            Optional<Long> lakeRetentionLag,
            boolean keepHeap) {
        this(oid, schemaName, tableName, primaryKeyCols, tierKeyCol, partitionScheme,
                lakeFormat, lakeTableRef, mode, publicationName, slotName,
                heapRetentionLag, lakeRetentionLag, keepHeap, DEFAULT_PROFILE);
    }

    public TableRegistration(
            long oid,
            String schemaName,
            String tableName,
            List<String> primaryKeyCols,
            String tierKeyCol,
            String partitionScheme,
            String lakeFormat,
            String lakeTableRef,
            TableMode mode,
            String publicationName,
            String slotName,
            Optional<Long> heapRetentionLag,
            Optional<Long> lakeRetentionLag) {
        this(oid, schemaName, tableName, primaryKeyCols, tierKeyCol, partitionScheme,
                lakeFormat, lakeTableRef, mode, publicationName, slotName,
                heapRetentionLag, lakeRetentionLag, false);
    }

    public TableRegistration(
            long oid,
            String schemaName,
            String tableName,
            List<String> primaryKeyCols,
            String tierKeyCol,
            String partitionScheme,
            String lakeFormat,
            String lakeTableRef) {
        this(oid, schemaName, tableName, primaryKeyCols, tierKeyCol, partitionScheme,
                lakeFormat, lakeTableRef,
                TableMode.TIERED, null, null, Optional.empty(), Optional.empty());
    }
}
