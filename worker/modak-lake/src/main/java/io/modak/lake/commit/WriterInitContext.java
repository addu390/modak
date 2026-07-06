package io.modak.lake.commit;

import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.TableId;

/**
 * Context for creating a {@link LakeWriter}. {@code lakeTableRef} comes from
 * {@code modak.tables.lake_table_ref}.
 */
public record WriterInitContext(
        TableId table, PartitionId partition, PartitionBounds bounds, String lakeTableRef) {}
