package io.tierdb.lake.commit;

import io.tierdb.common.LakeSnapshotId;
import java.util.Map;

/**
 * Result of a cold-store commit. Distinguishes the <b>committed</b>
 * snapshot from the <b>readable</b> one, since formats with deferred
 * visibility may lag and TierDB pins the readable one.
 */
public record LakeCommitResult(
        LakeSnapshotId committed,
        LakeSnapshotId readable,
        Map<String, String> publishProps) {

    public LakeCommitResult {
        publishProps = Map.copyOf(publishProps);
    }

    public static LakeCommitResult committedIsReadable(LakeSnapshotId s, Map<String, String> publishProps) {
        return new LakeCommitResult(s, s, publishProps);
    }
}
