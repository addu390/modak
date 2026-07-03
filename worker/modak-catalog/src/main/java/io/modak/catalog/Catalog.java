package io.modak.catalog;

import io.modak.common.Cutline;
import io.modak.common.DeltaBatch;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Facade over the {@code modak.*} catalog tables — the cross-language contract and
 * the only coordination channel with the Rust extension. The consistency-critical
 * operations ({@link #advanceCutline}, {@link #publishCompaction}) are single atomic
 * transactions.
 */
public interface Catalog {

    TableId register(TableRegistration registration);

    /**
     * Removes the registration row; cutline, partitions, delta, pins, and the
     * op journal cascade with it. Unknown tables are a no-op ({@code false}).
     */
    boolean unregister(TableId table);

    Optional<RegisteredTable> lookup(String schemaName, String tableName);

    Optional<RegisteredTable> get(TableId table);

    List<RegisteredTable> listTables();

    void initCutline(TableId table, TierKey t, LakeSnapshotId snapshot);

    Cutline readCutline(TableId table);

    void advanceCutline(TableId table, TierKey newT, LakeSnapshotId snapshot);

    /** Also merges {@code lakePropsPatch} (opaque lake metadata, e.g. Iceberg's metadata_location) in the same transaction. */
    void advanceCutline(TableId table, TierKey newT, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch);

    /**
     * The mirror frontier F: the highest WAL position whose changes are committed
     * to the lake. Empty for tiered tables and for mirrored tables whose initial
     * copy has not landed yet.
     */
    Optional<Lsn> readMirrorFrontier(TableId table);

    /**
     * Advance the mirror frontier and pinned lake snapshot together, merging
     * {@code lakePropsPatch} in the same transaction. Monotonic and guarded: a
     * regressing LSN or snapshot throws; the first call seeds a null frontier.
     */
    void advanceMirrorFrontier(TableId table, Lsn lsn, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch);

    /**
     * Raise only {@code T} (the heap retention line R of a mirrored table), leaving
     * the snapshot to the concurrently running mirror pump. Monotonic; a regression
     * throws. Tiered tables never use this — their T moves with S in
     * {@link #advanceCutline}.
     */
    void advanceRetentionLine(TableId table, TierKey newT);

    /**
     * Advance {@code S}, clear the folded delta rows (version-guarded: a row
     * re-corrected since folding survives), and merge {@code lakePropsPatch} — one
     * transaction. {@code T} is unchanged by compaction.
     */
    void publishCompaction(TableId table, LakeSnapshotId snapshot, DeltaBatch folded,
            Map<String, String> lakePropsPatch);

    /**
     * Publish a lake retention pass in one transaction: raise the retention line
     * {@code R}, purge delta rows with {@code tier_key < R}, drop DROPPED partition
     * rows below {@code R}, and (when the pass deleted lake data) advance {@code S}
     * with {@code lakePropsPatch}. Blocked while readers are pinned.
     */
    void publishRetention(TableId table, LakeSnapshotId snapshot, TierKey below,
            Map<String, String> lakePropsPatch);

    /** The retention line {@code R}; empty until the first retention pass. */
    Optional<TierKey> readRetentionLine(TableId table);

    /** Oldest pinned {@code (T, S)} across active read-pins; the cut-line when none. */
    Cutline readHorizon(TableId table);

    /**
     * Oldest pinned {@code (T, S)}, empty when no reader is pinned. Compaction needs
     * the distinction: any active pin blocks clearing delta rows (the pinned merge
     * still reads them), whereas no pins at all means clearing is trivially safe.
     */
    Optional<Cutline> pinnedHorizon(TableId table);

    /** Upsert an operation's phase in the crash-resume journal, keyed by {@code opId}. */
    void logOpPhase(UUID opId, TableId table, String opKind, String phase,
            LakeSnapshotId snapshot, String detailsJson);

    /** Non-terminal ops of {@code opKind} — work a crashed worker left behind, oldest first. */
    List<TieringOp> findIncompleteOps(TableId table, String opKind);

    void upsertPartition(PartitionId id, PartitionBounds bounds, PartitionState state);

    List<PartitionInfo> listPartitions(TableId table);

    void transition(PartitionId partition, PartitionState from, PartitionState to);
}
