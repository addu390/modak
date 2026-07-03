package io.modak.lake;

/**
 * Knobs for one lake maintenance pass. Bin-pack data files smaller than
 * {@code rewriteTargetBytes} once {@code rewriteMinInputFiles} accumulate, and
 * expire snapshots older than {@code snapshotRetentionMillis}, always retaining
 * {@code snapshotMinRetained} and never past the pinned reader horizon.
 */
public record MaintenanceConfig(
        long rewriteTargetBytes,
        int rewriteMinInputFiles,
        long snapshotRetentionMillis,
        int snapshotMinRetained) {}
