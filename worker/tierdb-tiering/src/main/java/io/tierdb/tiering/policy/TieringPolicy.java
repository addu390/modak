package io.tierdb.tiering.policy;

import io.tierdb.common.PartitionId;
import io.tierdb.common.TableId;
import java.time.Instant;
import java.util.List;

/**
 * Strategy: which sealed partitions are eligible to tier now.
 * Uses the partition <i>floor</i> for tier-eligibility,
 * distinct from the retention drop clock.
 */
public interface TieringPolicy {
    List<PartitionId> selectForTiering(TableId table, Instant now);
}
