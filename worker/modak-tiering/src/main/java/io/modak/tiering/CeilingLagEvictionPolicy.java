package io.modak.tiering;

import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.common.Cutline;
import java.util.Objects;
import java.util.function.Supplier;
import javax.sql.DataSource;

/**
 * Two-clock retention. Tiering measures from a partition's floor, the
 * destructive DROP from its <b>ceiling</b>, reclaiming only once the newest
 * possible row is {@code reclaimLag} behind the write frontier. Layered on the
 * seal gate, which keeps the correctness half (TIERED state, no pinned reader below).
 */
public final class CeilingLagEvictionPolicy implements EvictionPolicy {

    private final EvictionPolicy sealGate = new SealGatedEvictionPolicy();
    private final Supplier<Long> highWater;
    private final long reclaimLag;

    private boolean probed;
    private Long cachedHighWater;

    public CeilingLagEvictionPolicy(Supplier<Long> highWater, long reclaimLag) {
        this.highWater = Objects.requireNonNull(highWater);
        this.reclaimLag = reclaimLag;
    }

    /** The production shape: frontier probed from the hot table, once per cycle. */
    public static CeilingLagEvictionPolicy forJdbc(DataSource dataSource, RegisteredTable meta,
            long reclaimLag) {
        return new CeilingLagEvictionPolicy(() -> HotHighWater.query(dataSource, meta), reclaimLag);
    }

    @Override
    public boolean canReclaim(PartitionInfo partition, Cutline horizon) {
        if (!sealGate.canReclaim(partition, horizon)) {
            return false;
        }
        // A null frontier means an empty hot table: nothing is aging, stay conservative.
        Long frontier = memoizedHighWater();
        return frontier != null && partition.bounds().hi().value() <= frontier - reclaimLag;
    }

    private Long memoizedHighWater() {
        if (!probed) {
            cachedHighWater = highWater.get();
            probed = true;
        }
        return cachedHighWater;
    }
}
