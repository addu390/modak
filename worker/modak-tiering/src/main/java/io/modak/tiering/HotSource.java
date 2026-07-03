package io.modak.tiering;

import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.common.PartitionData;
import io.modak.common.PartitionId;

/**
 * The hot (Postgres) side of tiering. Reads a sealed partition out, physically
 * reclaims a tiered one. Reclaim is a partition <b>DROP/DETACH</b>, never a
 * row {@code DELETE}, because aging-out must stay invisible to any CDC mirror.
 */
public interface HotSource {

    /** Read all rows of one hot partition (its tier-key range is sealed, so this is stable). */
    PartitionData read(RegisteredTable table, PartitionInfo partition);

    /** Physically drop the partition. Idempotent: a missing partition is a no-op. */
    void dropPartition(RegisteredTable table, PartitionId partition);
}
