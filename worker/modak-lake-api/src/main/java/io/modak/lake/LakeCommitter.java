package io.modak.lake;

import io.modak.common.LakeSnapshotId;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Aggregates per-partition write results into one atomic cold-store snapshot commit
 * (Strategy per format), including the crash-recovery probe.
 */
public interface LakeCommitter<WriteResult, Committable> extends AutoCloseable {

    /** Returns {@code null} when the results contain nothing to commit (empty flush). */
    Committable toCommittable(List<WriteResult> results) throws IOException;

    /** Commit the cold-store snapshot. {@code snapshotProps} carries cut-line metadata. */
    LakeCommitResult commit(Committable committable, Map<String, String> snapshotProps) throws IOException;

    void abort(Committable committable) throws IOException;

    /** A snapshot this committer wrote but the catalog doesn't know (crash between commit and advance), or empty. */
    Optional<CommittedLakeSnapshot> getMissingLakeSnapshot(LakeSnapshotId lastKnownInCatalog)
            throws IOException;

    /**
     * Kind-filtered variant, since each protocol (tiering, mirror) must only
     * claim its own snapshots. The default keeps tiering's probe and finds
     * nothing for other kinds, whose crash resume falls back to an idempotent replay.
     */
    default Optional<CommittedLakeSnapshot> getMissingLakeSnapshot(
            LakeSnapshotId lastKnownInCatalog, String opKind) throws IOException {
        return LakeTieringProps.OP_KIND_TIERING.equals(opKind)
                ? getMissingLakeSnapshot(lastKnownInCatalog)
                : Optional.empty();
    }
}
