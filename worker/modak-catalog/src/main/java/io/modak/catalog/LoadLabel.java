package io.modak.catalog;

import io.modak.common.TableId;

/**
 * A row of {@code modak.load_labels}: one labeled Stream Load batch and its
 * recorded outcome. Commits atomically with the batch's data, so replaying
 * the label returns {@code resultJson} instead of re-applying.
 */
public record LoadLabel(
        TableId table,
        String label,
        LoadState state,
        String stagedFilesJson,
        String resultJson) {}
