package io.tierdb.common;

/**
 * The cut-line. Rows with {@code tier_key >= t} live in Postgres, rows below
 * in the cold base at version {@code snapshot}, overlaid by the delta.
 * {@code t} and {@code snapshot} always advance together as one atomic fact.
 */
public record Cutline(TierKey t, LakeSnapshotId snapshot) {}
