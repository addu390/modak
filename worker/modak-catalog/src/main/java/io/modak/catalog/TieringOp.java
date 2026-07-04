package io.modak.catalog;

import io.modak.common.LakeSnapshotId;
import io.modak.common.OpKind;
import io.modak.common.OpPhase;
import io.modak.common.TableId;
import java.util.Optional;
import java.util.UUID;

/** A row of {@code modak.tiering_log}: one op and the last phase it durably reached. */
public record TieringOp(
        UUID opId,
        TableId table,
        OpKind opKind,
        OpPhase phase,
        Optional<LakeSnapshotId> snapshot,
        String detailsJson) {
}
