package io.modak.worker;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.compaction.CompactionWorker;
import io.modak.compaction.JdbcCompactionPolicy;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeStoragePlugin;
import io.modak.common.Cutline;
import io.modak.common.TableId;
import io.modak.tiering.CeilingLagEvictionPolicy;
import io.modak.tiering.JdbcHotSource;
import io.modak.tiering.LagBasedTieringPolicy;
import io.modak.tiering.PartitionPremake;
import io.modak.tiering.PartitionSync;
import io.modak.tiering.ReclaimException;
import io.modak.tiering.TieringWorker;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.sql.DataSource;

/**
 * Fixed-interval scheduler over every registered table's tiering, compaction,
 * mirror, and maintenance cycles. Active/passive HA via an advisory-lock leader
 * lease; correctness never rests on the lease (advances are monotonic-guarded,
 * slots single-consumer) — it only prevents duplicate work.
 */
public final class WorkerDaemon {

    private static final long WORKER_LOCK_KEY = 0x6d6f64616bL; // "modak", arbitrary but stable

    private final WorkerConfig config;
    private final DataSource dataSource;
    private final JdbcCatalog catalog;
    private final PartitionSync partitionSync;
    private final Map<String, LakeStorage> lakes = new HashMap<>();
    private final Map<TableId, Cutline> lastLogged = new HashMap<>();
    private final Map<TableId, MirrorPump> mirrorPumps = new HashMap<>();
    private final Map<TableId, MirrorRetention> retentions = new HashMap<>();
    private final Map<TableId, Long> lastMaintenance = new HashMap<>();
    private final java.util.Set<TableId> copyAnnounced = new java.util.HashSet<>();
    private final java.util.Set<TableId> premakeSkipAnnounced = new java.util.HashSet<>();
    private final Metrics metrics = new Metrics();
    private final SeriesStore seriesStore = new SeriesStore();
    private final StatusSweep statusSweep;
    private MetricsServer metricsServer;

    private record MirrorPump(MirrorWorker worker, Thread thread) {}

    private volatile boolean running = true;
    private volatile boolean leading;
    private Connection lockConnection;
    private Thread loop;

    public WorkerDaemon(WorkerConfig config) {
        this.config = config;
        this.dataSource = config.dataSource();
        this.catalog = new JdbcCatalog(dataSource);
        this.partitionSync = new PartitionSync(dataSource, catalog);
        this.statusSweep = new StatusSweep(dataSource, metrics, seriesStore,
                config.slotWarnBytes(), config.deltaBacklogWarnRows());
    }

    public void start() {
        Log.info("starting: pg=%s warehouse=%s interval=%ds lag=%d",
                config.pgUrl(), config.warehouse(), config.cycleIntervalSeconds(),
                config.tieringLag());
        CatalogSchema.apply(dataSource);
        if (config.metricsPort() > 0) {
            try {
                metricsServer = MetricsServer.start(config.metricsPort(), metrics);
                Log.info("metrics on :%d/metrics", metricsServer.port());
            } catch (Exception e) {
                Log.error("metrics endpoint failed to start on :%d: %s",
                        config.metricsPort(), e);
            }
        }
        loop = new Thread(this::runLoop, "modak-daemon");
        loop.setDaemon(false);
        loop.start();
    }

    /** Orderly shutdown (tests); a crashed process reaches the same end state. */
    public void stop() throws InterruptedException {
        running = false;
        if (loop != null) {
            loop.interrupt();
            loop.join(30_000);
        }
        stepDown();
        if (metricsServer != null) {
            metricsServer.stop();
        }
    }

    public boolean isLeading() {
        return leading;
    }

    /** Embedders (the console binary) read these; the daemon stays the writer. */
    public Metrics metrics() {
        return metrics;
    }

    public SeriesStore seriesStore() {
        return seriesStore;
    }

    private void runLoop() {
        while (running) {
            try {
                campaign();
                if (!running) {
                    return;
                }
                Log.info("leader lease acquired");
                leading = true;
                metrics.gauge("modak_leader", 1);
                lead();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                Log.error("daemon loop failed (re-campaigning): %s", e);
            } finally {
                leading = false;
                metrics.gauge("modak_leader", 0);
                stepDown();
            }
        }
    }

    private void campaign() throws Exception {
        boolean announced = false;
        while (running) {
            lockConnection = dataSource.getConnection();
            if (tryAdvisoryLock(lockConnection)) {
                return;
            }
            lockConnection.close();
            lockConnection = null;
            if (!announced) {
                Log.info("standing by: another worker holds the lease (retrying every %ds)",
                        config.campaignIntervalSeconds());
                announced = true;
            }
            Thread.sleep(config.campaignIntervalSeconds() * 1000);
        }
    }

    private void lead() throws Exception {
        while (running) {
            if (!stillLeader()) {
                Log.error("leader lease lost (lock session gone) — stepping down");
                return;
            }
            cycleAll();
            Thread.sleep(config.cycleIntervalSeconds() * 1000);
        }
    }

    private boolean stillLeader() {
        try {
            return lockConnection != null && lockConnection.isValid(2);
        } catch (Exception e) {
            return false;
        }
    }

    private void stepDown() {
        for (Map.Entry<TableId, MirrorPump> e : mirrorPumps.entrySet()) {
            MirrorPump pump = e.getValue();
            pump.worker().stop();
            pump.thread().interrupt();
            try {
                pump.thread().join(10_000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        mirrorPumps.clear();
        retentions.clear();
        lakes.clear();
        lastLogged.clear();
        lastMaintenance.clear();
        copyAnnounced.clear();
        premakeSkipAnnounced.clear();
        if (lockConnection != null) {
            try {
                lockConnection.close();
            } catch (Exception ignored) {
            }
            lockConnection = null;
        }
    }

    private void cycleAll() {
        long started = System.nanoTime();
        try {
            java.util.List<RegisteredTable> tables = catalog.listTables();
            for (RegisteredTable table : tables) {
                cycleOne(table);
            }
            statusSweep.run(tables);
            metrics.gauge("modak_last_cycle_timestamp_seconds",
                    System.currentTimeMillis() / 1000.0);
        } catch (Exception e) {
            Log.error("cycle sweep failed: %s", e);
        }
        metrics.gauge("modak_cycle_duration_seconds", (System.nanoTime() - started) / 1e9);
    }

    private void cycleOne(RegisteredTable table) {
        if (table.mode() == TableMode.MIRRORED) {
            cycleMirrored(table);
            return;
        }
        String name = table.schemaName() + "." + table.tableName();
        try {
            premakeIfEnabled(table);
            int added = partitionSync.sync(table);
            if (added > 0) {
                Log.info("%s: registered %d new partition(s)", name, added);
            }
            LakeStorage lake = lakeFor(table.lakeFormat());

            try {
                new TieringWorker(catalog, lake, new JdbcHotSource(dataSource),
                        new LagBasedTieringPolicy(dataSource, catalog, config.tieringLag()),
                        CeilingLagEvictionPolicy.forJdbc(dataSource, table, config.reclaimLag()))
                        .runCycle(table.id(), Instant.now());
            } catch (ReclaimException e) {
                Log.error("%s: reclaim failed (data is safe; DROP retries next cycle): %s",
                        name, e);
            }

            new CompactionWorker(catalog, lake,
                    new JdbcCompactionPolicy(dataSource, catalog, config.compactionBatchSize()))
                    .runCycle(table.id(), Instant.now());

            new RetentionWorker(catalog, lake).runCycle(table);

            Cutline cut = catalog.readCutline(table.id());
            if (!cut.equals(lastLogged.put(table.id(), cut))) {
                Log.info("%s: cutline now T=%d S=%d",
                        name, cut.t().value(), cut.snapshot().id());
            }

            maintainIfDue(table);
        } catch (Exception e) {
            Log.error("%s: cycle failed (will retry next interval): %s", name, e);
            e.printStackTrace();
        }
    }

    private void premakeIfEnabled(RegisteredTable table) {
        if (config.premakePartitions() <= 0) {
            return;
        }
        String name = table.schemaName() + "." + table.tableName();
        try {
            var result = new PartitionPremake(dataSource, config.premakePartitions())
                    .premake(table);
            if (result.isEmpty()) {
                if (premakeSkipAnnounced.add(table.id())) {
                    Log.info("%s: no range partitions — premake skipped", name);
                }
                return;
            }
            if (result.get().outsideGrid()) {
                Log.error("%s: rows sit at or past the top range partition bound "
                        + "(a DEFAULT partition?) — premake cannot extend the grid past them",
                        name);
            }
            if (result.get().created() > 0) {
                Log.info("%s: premade %d future partition(s)", name, result.get().created());
            }
        } catch (Exception e) {
            Log.error("%s: partition premake failed (will retry next cycle): %s", name, e);
        }
    }

    private void maintainIfDue(RegisteredTable table) {
        long now = System.currentTimeMillis();
        Long last = lastMaintenance.get(table.id());
        if (last != null && now - last < config.maintenanceIntervalSeconds() * 1000) {
            return;
        }
        lastMaintenance.put(table.id(), now);
        String name = table.schemaName() + "." + table.tableName();
        try {
            var result = new MaintenanceWorker(catalog, lakeFor(table.lakeFormat()),
                    config.maintenanceConfig()).runCycle(table);
            if (!result.isNoop()) {
                Log.info("%s: maintenance rewrote %d file(s) into %d, expired %d snapshot(s)",
                        name, result.rewrittenFiles(), result.addedFiles(),
                        result.expiredSnapshots());
            }
        } catch (Exception e) {
            Log.error("%s: maintenance failed (will retry next due time): %s", name, e);
        }
    }

    private void cycleMirrored(RegisteredTable table) {
        String name = table.schemaName() + "." + table.tableName();
        try {
            // No frontier = initial copy still in flight; pumping would have no anchor.
            if (catalog.readMirrorFrontier(table.id()).isEmpty()) {
                if (copyAnnounced.add(table.id())) {
                    Log.info("%s: initial copy in progress — mirror cycles wait for it", name);
                }
                return;
            }
            copyAnnounced.remove(table.id());
            MirrorPump pump = mirrorPumps.get(table.id());
            if (pump != null && !pump.thread().isAlive() && pump.worker().diverged()) {
                return; // dead by destructive DDL: respawning would replay the same failure
            }
            if (pump == null || !pump.thread().isAlive()) {
                MirrorWorker worker = new MirrorWorker(catalog, lakeFor(table.lakeFormat()), table,
                        config.pgUrl(), config.pgUser(), config.pgPassword(),
                        config.mirrorBatchRows(), config.mirrorFlushMillis(),
                        config.mirrorMaxBufferedRows());
                Thread t = new Thread(worker, "modak-mirror-" + name);
                t.setDaemon(false);
                t.start();
                mirrorPumps.put(table.id(), new MirrorPump(worker, t));
                Log.info("%s: mirror pump started (slot=%s)", name, table.slotName());
            }

            if (table.heapRetentionLag().isPresent()) {
                premakeIfEnabled(table);
                int added = partitionSync.sync(table);
                if (added > 0) {
                    Log.info("%s: registered %d new partition(s)", name, added);
                }
                retentions.computeIfAbsent(table.id(),
                        id -> new MirrorRetention(dataSource, catalog)).run(table);
            }

            maintainIfDue(table);
        } catch (Exception e) {
            Log.error("%s: mirrored cycle failed (will retry next interval): %s", name, e);
        }
    }

    LakeStorage lakeFor(String format) {
        return lakes.computeIfAbsent(format, f -> {
            for (LakeStoragePlugin plugin : ServiceLoader.load(LakeStoragePlugin.class)) {
                if (plugin.identifier().equals(f)) {
                    return plugin.create(config.lakeConfig());
                }
            }
            throw new IllegalStateException("no LakeStoragePlugin for format: " + f);
        });
    }

    private static boolean tryAdvisoryLock(Connection c) throws Exception {
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(
                        "SELECT pg_try_advisory_lock(" + WORKER_LOCK_KEY + ")")) {
            rs.next();
            return rs.getBoolean(1);
        }
    }
}
