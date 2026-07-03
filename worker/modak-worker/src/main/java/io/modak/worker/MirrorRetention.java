package io.modak.worker;

import io.modak.catalog.Catalog;
import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.common.Cutline;
import io.modak.common.Lsn;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.tiering.HotHighWater;
import io.modak.tiering.JdbcHotSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * Heap retention for MIRRORED tables with {@code heap_retention_lag}: a partition is
 * dropped only after the mirror frontier passes an LSN recorded when it first
 * became age-eligible — proof the lake holds every row it ever had. The retention
 * line R is raised first, and pinned readers below it defer the drop.
 */
final class MirrorRetention {

    private final DataSource dataSource;
    private final Catalog catalog;
    private final JdbcHotSource hotSource;
    private final Map<PartitionId, Lsn> eligibleAt = new HashMap<>();

    MirrorRetention(DataSource dataSource, Catalog catalog) {
        this.dataSource = dataSource;
        this.catalog = catalog;
        this.hotSource = new JdbcHotSource(dataSource);
    }

    void run(RegisteredTable meta) {
        long lag = meta.heapRetentionLag().orElseThrow();
        TableId table = meta.id();
        Long highWater = HotHighWater.query(dataSource, meta);
        if (highWater == null) {
            return;
        }
        Optional<Lsn> frontier = catalog.readMirrorFrontier(table);
        if (frontier.isEmpty()) {
            return;
        }

        List<PartitionInfo> parts = new ArrayList<>(catalog.listPartitions(table));
        parts.sort((a, b) -> a.bounds().lo().compareTo(b.bounds().lo()));
        for (PartitionInfo p : parts) {
            if (p.state() == PartitionState.DROPPED) {
                continue;
            }
            if (p.bounds().hi().value() > highWater - lag) {
                continue;
            }
            Lsn recorded = eligibleAt.get(p.id());
            if (recorded == null) {
                eligibleAt.put(p.id(), currentWalLsn());
                continue;
            }
            if (frontier.get().compareTo(recorded) < 0) {
                continue;
            }
            drop(meta, p);
            eligibleAt.remove(p.id());
        }
    }

    private void drop(RegisteredTable meta, PartitionInfo p) {
        TableId table = meta.id();
        // Raise R first: readers now source these rows from the lake copy.
        catalog.advanceRetentionLine(table, p.bounds().hi());

        // A reader pinned below the new line still unions the heap side — wait it out.
        Optional<Cutline> pinned = catalog.pinnedHorizon(table);
        if (pinned.isPresent() && pinned.get().t().compareTo(p.bounds().hi()) < 0) {
            Log.info("%s.%s: retention drop of %s deferred (reader pinned below %d)",
                    meta.schemaName(), meta.tableName(), p.id().id(), p.bounds().hi().value());
            return;
        }

        // For a mirrored partition, "TIERED" is established by the CDC frontier proof.
        walkToTiered(p);
        hotSource.dropPartition(meta, p.id());
        catalog.transition(p.id(), PartitionState.TIERED, PartitionState.DROPPED);
        Log.info("%s.%s: dropped mirrored partition %s (retention)",
                meta.schemaName(), meta.tableName(), p.id().id());
    }

    private void walkToTiered(PartitionInfo p) {
        switch (p.state()) {
            case HOT -> {
                catalog.transition(p.id(), PartitionState.HOT, PartitionState.SEALING);
                catalog.transition(p.id(), PartitionState.SEALING, PartitionState.TIERING);
                catalog.transition(p.id(), PartitionState.TIERING, PartitionState.TIERED);
            }
            case SEALING -> {
                catalog.transition(p.id(), PartitionState.SEALING, PartitionState.TIERING);
                catalog.transition(p.id(), PartitionState.TIERING, PartitionState.TIERED);
            }
            case TIERING -> catalog.transition(p.id(), PartitionState.TIERING, PartitionState.TIERED);
            case TIERED -> { /* re-drive after a crash between walk and drop */ }
            default -> throw new IllegalStateException("unexpected state for " + p.id() + ": " + p.state());
        }
    }

    private Lsn currentWalLsn() {
        try (Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT pg_current_wal_lsn()::text")) {
            rs.next();
            return Lsn.fromPg(rs.getString(1));
        } catch (SQLException e) {
            throw new IllegalStateException("failed to read pg_current_wal_lsn()", e);
        }
    }
}
