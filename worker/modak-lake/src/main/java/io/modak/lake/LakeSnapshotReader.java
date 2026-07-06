package io.modak.lake;

import io.modak.common.LakeSnapshotId;
import java.io.IOException;

/**
 * Read side (ISP: split from the write ports). Scans the cold store at a pinned
 * snapshot, deletion-vector aware, with predicate/projection pushdown.
 */
public interface LakeSnapshotReader {
    RecordStream scan(LakeSnapshotId snapshot, Predicate pushdown, Projection projection) throws IOException;

    /** Opaque predicate pushed to the format reader. */
    interface Predicate {
        Predicate ALWAYS_TRUE = new Predicate() {};
    }

    /** Column projection, empty selects all. */
    record Projection(int[] columns) {
        public static final Projection ALL = new Projection(new int[0]);
    }

    /** A closable stream of records materialized by the format reader. */
    interface RecordStream extends AutoCloseable {}
}
