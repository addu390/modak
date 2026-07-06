package io.modak.lake.iceberg;

import io.modak.common.LakeSnapshotId;
import io.modak.lake.commit.LakeCommitResult;
import java.util.Map;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;

/**
 * The commit epilogue every snapshot-producing operation shares: stamp the
 * snapshot props, commit, refresh, and wrap the result with publish props.
 */
final class IcebergSnapshotCommit {

    private final Table table;

    IcebergSnapshotCommit(Table table) {
        this.table = table;
    }

    LakeCommitResult apply(SnapshotUpdate<?> update, Map<String, String> snapshotProps) {
        snapshotProps.forEach(update::set);
        update.commit();
        table.refresh();
        return LakeCommitResult.committedIsReadable(
                new LakeSnapshotId(table.currentSnapshot().sequenceNumber()),
                IcebergPublish.props(table));
    }
}
