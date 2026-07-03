package io.modak.tiering;

import io.modak.common.LakeSnapshotId;
import io.modak.common.PartitionData;
import io.modak.common.RowBatchData;
import io.modak.lake.CommittedLakeSnapshot;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeCommitter;
import io.modak.lake.LakeSnapshotReader;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringFactory;
import io.modak.lake.LakeTieringProps;
import io.modak.lake.LakeWriter;
import io.modak.lake.MergeWriter;
import io.modak.lake.WriterInitContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * In-memory lake for worker tests: monotonic snapshot ids with summaries kept.
 * {@code failBeforeCommit} / {@code failAfterCommit} inject crashes around the commit.
 */
final class FakeLake implements LakeStorage {

    record CommittedSnapshot(long id, Map<String, String> props, List<Object[]> rows) {}

    final List<CommittedSnapshot> snapshots = new ArrayList<>();
    long nextSnapshotId = 1;

    boolean failBeforeCommit;
    boolean failAfterCommit;
    int writersCreated;
    int abortedCommittables;

    /** Simulates a snapshot the catalog doesn't know about (e.g. a foreign committer). */
    void seedSnapshot(Map<String, String> props, List<Object[]> rows) {
        snapshots.add(new CommittedSnapshot(nextSnapshotId++, Map.copyOf(props), rows));
    }

    List<Object[]> allRows() {
        List<Object[]> out = new ArrayList<>();
        for (CommittedSnapshot s : snapshots) {
            out.addAll(s.rows());
        }
        return out;
    }

    @Override
    public String tableRef(String schema, String table) {
        return "/fake/" + schema + "." + table;
    }

    @Override
    public String createTableIfAbsent(String ref, List<io.modak.common.RowBatchData.Column> cols,
            java.util.Set<String> requiredCols, String tierKeyCol, long partitionWidth) {
        return ref + "/metadata/v0.metadata.json";
    }

    @Override
    public void dropTable(String ref) {
        throw new UnsupportedOperationException("not needed for tiering tests");
    }

    @Override
    public LakeTieringFactory<List<Object[]>, List<Object[]>> tieringFactory() {
        return new LakeTieringFactory<>() {
            @Override
            public LakeWriter<List<Object[]>> createWriter(WriterInitContext ctx) {
                writersCreated++;
                return new FakeWriter();
            }

            @Override
            public LakeCommitter<List<Object[]>, List<Object[]>> createCommitter(
                    CommitterInitContext ctx) {
                return new FakeCommitter();
            }
        };
    }

    @Override
    public MergeWriter mergeWriter(CommitterInitContext ctx) {
        throw new UnsupportedOperationException("not needed for tiering tests");
    }

    @Override
    public LakeSnapshotReader snapshotReader() {
        throw new UnsupportedOperationException("not needed for tiering tests");
    }

    @Override
    public void evolveSchema(CommitterInitContext ctx,
            List<io.modak.common.RowBatchData.Column> addColumns) {
        throw new UnsupportedOperationException("not needed for tiering tests");
    }

    @Override
    public io.modak.lake.MaintenanceResult maintain(CommitterInitContext ctx,
            io.modak.lake.MaintenanceConfig config,
            io.modak.common.LakeSnapshotId oldestPinnedSnapshot,
            java.util.Map<String, String> snapshotProps) {
        return io.modak.lake.MaintenanceResult.NOOP;
    }

    @Override
    public io.modak.lake.LakeCommitResult expireBelow(CommitterInitContext ctx,
            String tierKeyCol, long boundary, java.util.Map<String, String> snapshotProps) {
        return null;
    }

    private static final class FakeWriter implements LakeWriter<List<Object[]>> {
        private final List<Object[]> rows = new ArrayList<>();

        @Override
        public void write(PartitionData data) {
            rows.addAll(((RowBatchData) data).rows());
        }

        @Override
        public List<Object[]> complete() {
            return rows;
        }

        @Override
        public void close() {}
    }

    private final class FakeCommitter implements LakeCommitter<List<Object[]>, List<Object[]>> {
        @Override
        public List<Object[]> toCommittable(List<List<Object[]>> results) {
            List<Object[]> all = new ArrayList<>();
            results.forEach(all::addAll);
            return all.isEmpty() ? null : all;
        }

        @Override
        public LakeCommitResult commit(List<Object[]> committable, Map<String, String> snapshotProps)
                throws IOException {
            if (failBeforeCommit) {
                throw new IOException("injected crash BEFORE the lake commit");
            }
            long id = nextSnapshotId++;
            snapshots.add(new CommittedSnapshot(id, Map.copyOf(snapshotProps), committable));
            if (failAfterCommit) {
                throw new IllegalStateException("injected crash AFTER the lake commit");
            }
            return LakeCommitResult.committedIsReadable(new LakeSnapshotId(id), publishProps(id));
        }

        @Override
        public void abort(List<Object[]> committable) {
            abortedCommittables++;
        }

        @Override
        public Optional<CommittedLakeSnapshot> getMissingLakeSnapshot(LakeSnapshotId lastKnown) {
            for (int i = snapshots.size() - 1; i >= 0; i--) {
                CommittedSnapshot s = snapshots.get(i);
                if (s.props().containsKey(LakeTieringProps.OP_ID) && s.id() > lastKnown.id()) {
                    return Optional.of(new CommittedLakeSnapshot(
                            new LakeSnapshotId(s.id()), s.props(), publishProps(s.id())));
                }
            }
            return Optional.empty();
        }

        @Override
        public void close() {}
    }

    private static Map<String, String> publishProps(long snapshotId) {
        Map<String, String> props = new HashMap<>();
        props.put("metadata_location", "/fake/metadata/" + snapshotId + ".metadata.json");
        return props;
    }
}
