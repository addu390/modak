package io.modak.worker;

import io.modak.catalog.RegisteredTable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * One observability pass per sweep, covering per-table gauges, the slot WAL
 * guard, the delta-backlog alert, and expired read-pin cleanup. Guards are
 * alert-only (WARN at the threshold, ERROR with a runbook at 4x), never destructive.
 */
final class StatusSweep {

    private final DataSource dataSource;
    private final Metrics metrics;
    private final SeriesStore series;
    private final long slotWarnBytes;
    private final long deltaBacklogWarnRows;

    private final Map<Long, Long> lastSnapshot = new HashMap<>();
    private final Map<Long, Long> lastFrontier = new HashMap<>();

    StatusSweep(DataSource dataSource, Metrics metrics, SeriesStore series,
            long slotWarnBytes, long deltaBacklogWarnRows) {
        this.dataSource = dataSource;
        this.metrics = metrics;
        this.series = series;
        this.slotWarnBytes = slotWarnBytes;
        this.deltaBacklogWarnRows = deltaBacklogWarnRows;
    }

    void run(List<RegisteredTable> tables) {
        Map<Long, String> names = new HashMap<>();
        for (RegisteredTable t : tables) {
            names.put(t.id().oid(), t.schemaName() + "." + t.tableName());
        }
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            long currentWal;
            try (ResultSet rs = s.executeQuery(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')")) {
                rs.next();
                currentWal = rs.getLong(1);
            }
            expiredPins(s);
            deltaBacklog(s, names);
            cutlines(s, names, currentWal);
            slots(s);
        } catch (Exception e) {
            Log.error("status sweep failed: %s", e);
        }
    }

    private void expiredPins(Statement s) throws Exception {
        int deleted = s.executeUpdate(
                "DELETE FROM modak.read_pins WHERE expires_at <= now()");
        if (deleted > 0) {
            metrics.add("modak_expired_pins_deleted_total", deleted);
            Log.info("deleted %d expired read pin(s)", deleted);
        }
    }

    private void deltaBacklog(Statement s, Map<Long, String> names) throws Exception {
        Map<Long, Long> counts = new HashMap<>();
        try (ResultSet rs = s.executeQuery(
                "SELECT table_id, count(*) FROM modak.delta GROUP BY 1")) {
            while (rs.next()) {
                counts.put(rs.getLong(1), rs.getLong(2));
            }
        }
        names.forEach((oid, name) -> {
            long backlog = counts.getOrDefault(oid, 0L);
            metrics.gauge(
                    Metrics.series("modak_delta_backlog_rows", "table", name), backlog);
            series.record("delta_backlog|" + name, backlog);
            if (backlog >= 4 * deltaBacklogWarnRows) {
                Log.error("%s: delta backlog is %d row(s) (>= 4x the %d threshold). "
                        + "Compaction is not folding corrections into the lake. Runbook: "
                        + "check for a long-held read pin blocking compaction "
                        + "(SELECT * FROM modak.read_pins), then check the worker log for "
                        + "compaction failures; raise MODAK_COMPACTION_BATCH to drain faster.",
                        name, backlog, deltaBacklogWarnRows);
            } else if (backlog >= deltaBacklogWarnRows) {
                Log.warn("%s: delta backlog is %d row(s) (threshold %d), compaction is "
                        + "behind the correction rate", name, backlog, deltaBacklogWarnRows);
            }
        });
    }

    private void cutlines(Statement s, Map<Long, String> names, long currentWal)
            throws Exception {
        try (ResultSet rs = s.executeQuery(
                "SELECT table_id, tier_key_hi, lake_snapshot_id, replicated_lsn "
                        + "FROM modak.cutline")) {
            while (rs.next()) {
                String name = names.get(rs.getLong(1));
                if (name == null) {
                    continue;
                }
                metrics.gauge(Metrics.series("modak_cutline_tier_key", "table", name),
                        rs.getLong(2));
                long snapshot = rs.getLong(3);
                metrics.gauge(Metrics.series("modak_cutline_snapshot", "table", name), snapshot);
                Long prevS = lastSnapshot.put(rs.getLong(1), snapshot);
                if (prevS != null && snapshot > prevS) {
                    metrics.add(Metrics.series("modak_lake_commits_total", "table", name),
                            snapshot - prevS);
                }
                series.record("lake_commits|" + name,
                        prevS == null ? 0 : Math.max(0, snapshot - prevS));

                long frontier = rs.getLong(4);
                if (!rs.wasNull()) {
                    long lag = Math.max(0, currentWal - frontier);
                    metrics.gauge(Metrics.series("modak_mirror_lag_bytes", "table", name), lag);
                    series.record("mirror_lag_bytes|" + name, lag);
                    Long prevF = lastFrontier.put(rs.getLong(1), frontier);
                    if (prevF != null && frontier > prevF) {
                        metrics.increment(
                                Metrics.series("modak_mirror_flushes_total", "table", name));
                    }
                }
            }
        }
    }

    private void slots(Statement s) throws Exception {
        try (ResultSet rs = s.executeQuery(
                "SELECT slot_name, active, "
                        + "COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0) "
                        + "FROM pg_replication_slots WHERE slot_name LIKE 'modak\\_%'")) {
            while (rs.next()) {
                String slot = rs.getString(1);
                boolean active = rs.getBoolean(2);
                long retained = rs.getLong(3);
                metrics.gauge(Metrics.series("modak_slot_active", "slot", slot), active ? 1 : 0);
                metrics.gauge(
                        Metrics.series("modak_slot_retained_wal_bytes", "slot", slot), retained);
                series.record("slot_wal_bytes|" + slot, retained);
                if (retained >= 4 * slotWarnBytes) {
                    Log.error("slot %s retains %d bytes of WAL (>= 4x the %d-byte threshold). "
                            + "Postgres cannot recycle this WAL until the slot advances. "
                            + "Runbook: if the mirror worker is down, restart it; if the table "
                            + "was abandoned, unregister it (drop the modak.tables row, then "
                            + "SELECT pg_drop_replication_slot('%s')). Consider setting "
                            + "max_slot_wal_keep_size as a hard cap.",
                            slot, retained, slotWarnBytes, slot);
                } else if (retained >= slotWarnBytes) {
                    Log.warn("slot %s retains %d bytes of WAL (threshold %d), the mirror "
                            + "consumer is behind or stopped, WAL will accumulate until it "
                            + "catches up", slot, retained, slotWarnBytes);
                }
            }
        }
    }
}
