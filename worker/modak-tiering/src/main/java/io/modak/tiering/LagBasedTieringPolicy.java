package io.modak.tiering;

import io.modak.catalog.Catalog;
import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * Tier every partition wholly older than {@code (max hot tier-key) - lag}. Lag is
 * measured against the data's own high-water mark, not the wall clock, so any
 * tier-key unit works. Selects the contiguous run from the cut-line, the
 * gap-free shape {@link TieringWorker} requires.
 */
public final class LagBasedTieringPolicy implements TieringPolicy {

    private final DataSource dataSource;
    private final Catalog catalog;
    private final long lag;

    public LagBasedTieringPolicy(DataSource dataSource, Catalog catalog, long lag) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.catalog = Objects.requireNonNull(catalog);
        this.lag = lag;
    }

    @Override
    public List<PartitionId> selectForTiering(TableId table, Instant now) {
        RegisteredTable meta = catalog.get(table)
                .orElseThrow(() -> new TieringException("table not registered: " + table));
        Long highWater = HotHighWater.query(dataSource, meta);
        if (highWater == null) {
            return List.of();
        }
        long threshold = highWater - lag;
        long cutT = catalog.readCutline(table).t().value();

        List<PartitionInfo> candidates = new ArrayList<>();
        for (PartitionInfo p : catalog.listPartitions(table)) {
            boolean tierable = p.state() == PartitionState.HOT
                    || p.state() == PartitionState.SEALING
                    || p.state() == PartitionState.TIERING;
            if (tierable && p.bounds().hi().value() <= threshold) {
                candidates.add(p);
            }
        }
        candidates.sort((a, b) -> a.bounds().lo().compareTo(b.bounds().lo()));

        // Contiguous run from the cut-line, anything after a gap waits for later cycles.
        List<PartitionId> selected = new ArrayList<>();
        long expectedLo = cutT;
        for (PartitionInfo p : candidates) {
            if (p.bounds().lo().value() > expectedLo) {
                break;
            }
            if (p.bounds().hi().value() > expectedLo) {
                selected.add(p.id());
                expectedLo = p.bounds().hi().value();
            }
        }
        return selected;
    }

}
