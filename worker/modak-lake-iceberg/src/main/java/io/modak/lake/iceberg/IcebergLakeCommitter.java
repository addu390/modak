package io.modak.lake.iceberg;

import io.modak.common.LakeSnapshotId;
import io.modak.lake.CommittedLakeSnapshot;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeCommitter;
import io.modak.lake.LakeTieringProps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

/**
 * Commits one tiering op's data files as ONE Iceberg snapshot, stamped with the
 * {@link LakeTieringProps} crash resume recovers from. {@link LakeSnapshotId}
 * carries the Iceberg <b>sequence number</b>, not the snapshot id — the catalog's
 * monotonic guards need an ordered value and snapshot ids are random longs.
 */
final class IcebergLakeCommitter implements LakeCommitter<IcebergWriteResult, IcebergCommittable> {

    private final Table table;

    IcebergLakeCommitter(Table table) {
        this.table = table;
    }

    @Override
    public IcebergCommittable toCommittable(List<IcebergWriteResult> results) {
        List<org.apache.iceberg.DataFile> all = new ArrayList<>();
        for (IcebergWriteResult r : results) {
            all.addAll(r.dataFiles());
        }
        return all.isEmpty() ? null : new IcebergCommittable(all);
    }

    @Override
    public LakeCommitResult commit(IcebergCommittable committable, Map<String, String> snapshotProps)
            throws IOException {
        try {
            table.refresh();
            AppendFiles append = table.newAppend();
            committable.dataFiles().forEach(append::appendFile);
            snapshotProps.forEach(append::set);
            append.commit();

            table.refresh();
            Snapshot committed = latestSnapshotOf(
                    snapshotProps.get(LakeTieringProps.OP_ID),
                    snapshotProps.getOrDefault(
                            LakeTieringProps.OP_KIND, LakeTieringProps.OP_KIND_TIERING));
            if (committed == null) {
                throw new IOException("commit succeeded but its snapshot was not found (op "
                        + snapshotProps.get(LakeTieringProps.OP_ID) + ")");
            }
            return LakeCommitResult.committedIsReadable(
                    new LakeSnapshotId(committed.sequenceNumber()), publishProps());
        } catch (RuntimeException e) {
            throw new IOException("failed to commit to Iceberg table " + table.name(), e);
        }
    }

    @Override
    public void abort(IcebergCommittable committable) {
        List<String> paths = committable.dataFiles().stream()
                .map(f -> f.path().toString())
                .toList();
        CatalogUtil.deleteFiles(table.io(), paths, "data file", true);
    }

    @Override
    public Optional<CommittedLakeSnapshot> getMissingLakeSnapshot(LakeSnapshotId lastKnownInCatalog)
            throws IOException {
        return getMissingLakeSnapshot(lastKnownInCatalog, LakeTieringProps.OP_KIND_TIERING);
    }

    @Override
    public Optional<CommittedLakeSnapshot> getMissingLakeSnapshot(LakeSnapshotId lastKnownInCatalog,
            String opKind) throws IOException {
        table.refresh();
        Snapshot latestModak = latestSnapshotOf(null, opKind);
        if (latestModak == null) {
            return Optional.empty();
        }
        if (latestModak.sequenceNumber() <= lastKnownInCatalog.id()) {
            return Optional.empty();
        }
        return Optional.of(new CommittedLakeSnapshot(
                new LakeSnapshotId(latestModak.sequenceNumber()),
                latestModak.summary(),
                publishProps()));
    }

    @Override
    public void close() {}

    private Snapshot latestSnapshotOf(String opId) {
        return latestSnapshotOf(opId, LakeTieringProps.OP_KIND_TIERING);
    }

    // Only same-kind snapshots count: compaction folds, mirror applies and tiering
    // commits all carry an op-id, and each resume protocol must only claim its own.
    private Snapshot latestSnapshotOf(String opId, String opKind) {
        Snapshot best = null;
        for (Snapshot s : table.snapshots()) {
            Map<String, String> summary = s.summary();
            if (summary == null
                    || !opKind.equals(summary.get(LakeTieringProps.OP_KIND))) {
                continue;
            }
            String stamped = summary.get(LakeTieringProps.OP_ID);
            boolean match = opId == null ? stamped != null : opId.equals(stamped);
            if (match && (best == null || s.sequenceNumber() > best.sequenceNumber())) {
                best = s;
            }
        }
        return best;
    }

    private Map<String, String> publishProps() {
        Map<String, String> props = new HashMap<>();
        var metadata = ((BaseTable) table).operations().current();
        props.put("metadata_location", metadata.metadataFileLocation());
        if (metadata.currentSnapshot() != null) {
            props.put("snapshot_id", Long.toString(metadata.currentSnapshot().snapshotId()));
        }
        return props;
    }
}
