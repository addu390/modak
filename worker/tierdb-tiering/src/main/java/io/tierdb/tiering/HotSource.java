package io.tierdb.tiering;

import io.tierdb.catalog.PartitionInfo;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.PartitionData;
import io.tierdb.common.PartitionId;

/**
 * The hot (Postgres) side of tiering. Reads a sealed partition out, physically
 * reclaims a tiered one. Reclaim is a partition <b>DROP/DETACH</b>, never a
 * row {@code DELETE}, because aging-out must stay invisible to any CDC mirror.
 */
public interface HotSource {

    PartitionData read(RegisteredTable table, PartitionInfo partition);

    void dropPartition(RegisteredTable table, PartitionId partition);

    default void attachColdMirror(RegisteredTable table, PartitionId partition) {}
}
