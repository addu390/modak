package io.modak.lake;

import io.modak.common.DeltaBatch;
import java.io.IOException;
import java.util.Map;

/**
 * Folds a batch of {@code modak.delta} entries into the cold base as one atomic
 * snapshot, newest-wins by PK. Re-folding is idempotent by construction
 * (delete-by-PK then re-insert the same image), so unpublished folds are
 * simply redone after a crash.
 */
public interface MergeWriter {
    /** {@code snapshotProps} are stamped onto the committed snapshot (op-id etc). */
    LakeCommitResult applyDelta(DeltaBatch batch, Map<String, String> snapshotProps) throws IOException;
}
