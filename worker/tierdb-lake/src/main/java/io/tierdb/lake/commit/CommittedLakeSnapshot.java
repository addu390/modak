package io.tierdb.lake.commit;

import io.tierdb.common.LakeSnapshotId;
import java.util.Map;

/**
 * A snapshot found committed in the lake, reconstructed from the format's own
 * metadata, so a crashed op can complete its cut-line advance without
 * re-committing data.
 */
public record CommittedLakeSnapshot(
        LakeSnapshotId readable,
        Map<String, String> snapshotProps,
        Map<String, String> publishProps) {

    public CommittedLakeSnapshot {
        snapshotProps = Map.copyOf(snapshotProps);
        publishProps = Map.copyOf(publishProps);
    }
}
