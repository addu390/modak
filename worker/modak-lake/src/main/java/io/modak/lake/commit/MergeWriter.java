package io.modak.lake.commit;

import io.modak.common.DeltaBatch;
import java.io.IOException;
import java.util.Map;

/**
 * Folds a batch of {@code modak.delta} entries into the cold base as one
 * atomic snapshot, newest-wins by PK.
 */
public interface MergeWriter {
    LakeCommitResult applyDelta(DeltaBatch batch, Map<String, String> snapshotProps) throws IOException;
}
