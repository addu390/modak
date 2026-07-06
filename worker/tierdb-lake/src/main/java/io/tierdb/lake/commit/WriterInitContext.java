package io.tierdb.lake.commit;

import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.TableId;

/**
 * Context for creating a {@link LakeWriter}. {@code lakeTableRef} comes from
 * {@code tierdb.tables.lake_table_ref}.
 */
public record WriterInitContext(
        TableId table, PartitionId partition, PartitionBounds bounds, String lakeTableRef) {}
