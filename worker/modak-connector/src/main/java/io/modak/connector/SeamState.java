package io.modak.connector;

import java.util.List;

/**
 * One captured seam: the registration row plus the cut-line the pin holds.
 * {@code pinId} is null for unpinned captures. {@code snapshotId} and
 * {@code metadataLocation} stay null until the worker's first lake commit.
 * Consumers dispatch their cold branch on {@code lakeFormat}.
 */
public record SeamState(
        long tableId,
        List<String> primaryKeyCols,
        String tierKeyCol,
        String mode,
        String lakeFormat,
        String lakeTableRef,
        String metadataLocation,
        Long snapshotId,
        Long retentionLag,
        long tierKeyHi,
        Long pinId) {

    public boolean heapIsComplete() {
        return "mirrored".equals(mode) && retentionLag == null;
    }
}
