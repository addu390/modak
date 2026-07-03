package io.modak.lake;

/** What one maintenance pass did, all-zero means the pass was a no-op. */
public record MaintenanceResult(int rewrittenFiles, int addedFiles, int expiredSnapshots) {

    public static final MaintenanceResult NOOP = new MaintenanceResult(0, 0, 0);

    public boolean isNoop() {
        return rewrittenFiles == 0 && addedFiles == 0 && expiredSnapshots == 0;
    }
}
