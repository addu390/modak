package io.modak.lake;

import io.modak.common.TableId;

/**
 * Context for creating a {@link LakeCommitter}. {@code lakeTableRef} comes
 * from {@code modak.tables.lake_table_ref}.
 */
public record CommitterInitContext(TableId table, String lakeTableRef) {}
