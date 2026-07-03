package io.modak.catalog;

import io.modak.common.LakeSnapshotId;
import io.modak.common.TableId;
import java.util.Optional;
import java.util.UUID;

/**
 * A row of {@code modak.tiering_log}, one op and the last phase it durably
 * reached (flushing, committed, or advanced). The abandoned phase means the
 * worker crashed before the lake commit and the op is safe to redo.
 */
public record TieringOp(
        UUID opId,
        TableId table,
        String opKind,
        String phase,
        Optional<LakeSnapshotId> snapshot,
        String detailsJson) {

    public static final String KIND_TIERING = "tiering";
    public static final String KIND_COMPACTION = "compaction";
    public static final String KIND_MAINTENANCE = "maintenance";
    public static final String KIND_RETENTION = "retention";
    public static final String KIND_INGEST = "ingest";

    public static final String PHASE_FLUSHING = "flushing";
    public static final String PHASE_COMMITTED = "committed";
    public static final String PHASE_ADVANCED = "advanced";
    public static final String PHASE_ABANDONED = "abandoned";

    public static boolean isTerminal(String phase) {
        return PHASE_ADVANCED.equals(phase) || PHASE_ABANDONED.equals(phase);
    }
}
