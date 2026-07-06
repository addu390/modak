package io.tierdb.lake.commit;

import io.tierdb.common.TableId;

/**
 * Context for creating a {@link LakeCommitter}. {@code lakeTableRef} comes
 * from {@code tierdb.tables.lake_table_ref}.
 */
public record CommitterInitContext(TableId table, String lakeTableRef) {}
