package io.tierdb.catalog;

import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.OpKind;
import io.tierdb.common.OpPhase;
import io.tierdb.common.TableId;
import java.util.Optional;
import java.util.UUID;

/** A row of {@code tierdb.op_log}: one op and the last phase it durably reached. */
public record TieringOp(
        UUID opId,
        TableId table,
        OpKind opKind,
        OpPhase phase,
        Optional<LakeSnapshotId> snapshot,
        String detailsJson) {
}
