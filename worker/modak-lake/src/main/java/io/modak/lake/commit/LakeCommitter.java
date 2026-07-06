package io.modak.lake.commit;

import io.modak.common.LakeSnapshotId;
import io.modak.common.OpKind;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Aggregates per-partition write results into one atomic cold-store snapshot commit
 * (Strategy per format), including the crash-recovery probe.
 */
public interface LakeCommitter<WriteResult, Committable> extends AutoCloseable {

    Committable toCommittable(List<WriteResult> results) throws IOException;

    LakeCommitResult commit(Committable committable, Map<String, String> snapshotProps) throws IOException;

    void abort(Committable committable) throws IOException;

    Optional<CommittedLakeSnapshot> getMissingLakeSnapshot(LakeSnapshotId lastKnownInCatalog)
            throws IOException;

    default Optional<CommittedLakeSnapshot> getMissingLakeSnapshot(
            LakeSnapshotId lastKnownInCatalog, OpKind opKind) throws IOException {
        return opKind == OpKind.TIERING
                ? getMissingLakeSnapshot(lastKnownInCatalog)
                : Optional.empty();
    }
}
