package io.tierdb.worker;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.TableMode;
import io.tierdb.common.Cutline;
import io.tierdb.common.TableId;
import io.tierdb.lake.commit.LakeCommitLock;
import io.tierdb.compaction.CompactionWorker;
import io.tierdb.compaction.JdbcCompactionPolicy;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.ColdTableSpec;
import io.tierdb.lake.LakeTable;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.maintain.MaintenanceEngine;
import io.tierdb.tiering.JdbcHotSource;
import io.tierdb.tiering.PartitionPremake;
import io.tierdb.tiering.PartitionSync;
import io.tierdb.tiering.ReclaimException;
import io.tierdb.tiering.TieringWorker;
import io.tierdb.tiering.policy.CeilingLagEvictionPolicy;
import io.tierdb.tiering.policy.LagBasedTieringPolicy;
import io.tierdb.worker.http.LoadEndpoint;
import io.tierdb.worker.http.Metrics;
import io.tierdb.worker.http.MetricsServer;
import io.tierdb.worker.http.SeriesStore;
import io.tierdb.worker.ops.EmbeddedMaintenanceEngine;
import io.tierdb.worker.ops.LakeStatsCollector;
import io.tierdb.worker.ops.LoadAdoptionWorker;
import io.tierdb.worker.ops.MaintenanceWorker;
import io.tierdb.worker.ops.MirrorRetention;
import io.tierdb.worker.ops.MirrorWorker;
import io.tierdb.worker.ops.RetentionWorker;
import io.tierdb.worker.ops.StagedFileJanitor;
import io.tierdb.worker.ops.StatusSweep;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Fixed-interval scheduler over every registered table's tiering,
 * compaction, mirror, and maintenance cycles. Active/passive HA via an
 * advisory-lock leader lease.
 */
public final class WorkerDaemon {

    private final WorkerConfig config;
    private final DataSource dataSource;
    private final JdbcCatalog catalog;
    private final LeaderLease lease;
    private final PartitionSync partitionSync;
    private final LakeStorages lakes;
    private final Map<TableId, Cutline> lastLogged = new HashMap<>();
    private final Map<TableId, MirrorPump> mirrorPumps = new HashMap<>();
    private final Map<TableId, MirrorRetention> retentions = new HashMap<>();
    private final Map<TableId, Long> lastMaintenance = new HashMap<>();
    private final Map<TableId, Long> lastLakeStats = new HashMap<>();
    private final MaintenanceEngine maintenanceEngine = new EmbeddedMaintenanceEngine();
    private final LakeStatsCollector lakeStatsCollector;
    private final StagedFileJanitor stagedFileJanitor;
    private final java.util.Set<TableId> copyAnnounced = new java.util.HashSet<>();
    private final java.util.Set<TableId> premakeSkipAnnounced = new java.util.HashSet<>();
    private final Metrics metrics = new Metrics();
    private final SeriesStore seriesStore = new SeriesStore();
    private final StatusSweep statusSweep;
    private MetricsServer metricsServer;

    private record MirrorPump(MirrorWorker worker, Thread thread) {}

    private volatile boolean running = true;
    private volatile boolean leading;
    private Thread loop;

    public WorkerDaemon(WorkerConfig config) {
        this.config = config;
        this.dataSource = config.dataSource();
        this.lease = new LeaderLease(dataSource);
        this.catalog = new JdbcCatalog(dataSource);
        this.lakes = new LakeStorages(config, catalog);
        this.partitionSync = new PartitionSync(dataSource, catalog);
        this.statusSweep = new StatusSweep(dataSource, metrics, seriesStore,
                config.slotWarnBytes(), config.deltaBacklogWarnRows());
        this.lakeStatsCollector = new LakeStatsCollector(dataSource);
        this.stagedFileJanitor = new StagedFileJanitor(dataSource);
    }

    public void start() {
        Log.info("starting: pg=%s warehouse=%s interval=%ds lag=%s",
                config.pgUrl(), config.warehouse(), config.cycleIntervalSeconds(),
                config.tieringLag());
        CatalogSchema.apply(dataSource);
        if (config.metricsPort() > 0) {
            try {
                LoadEndpoint load = LoadEndpoint.fromConfig(config, metrics);
                metricsServer = MetricsServer.start(config.metricsPort(), metrics, load);
                Log.info("metrics on :%d/metrics%s", metricsServer.port(),
                        load != null ? ", stream load at " + LoadEndpoint.PATH : "");
            } catch (Exception e) {
                Log.error("metrics endpoint failed to start on :%d: %s",
                        config.metricsPort(), e);
            }
        }
        loop = new Thread(this::runLoop, "tierdb-daemon");
        loop.setDaemon(false);
        loop.start();
    }

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
                metrics.gauge("tierdb_leader", 1);
                lead();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                Log.error("daemon loop failed (re-campaigning): %s", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            } finally {
                leading = false;
                metrics.gauge("tierdb_leader", 0);
                stepDown();
            }
        }
    }

    private void campaign() throws Exception {
        boolean announced = false;
        while (running) {
            if (lease.tryAcquire()) {
                return;
            }
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
            if (!lease.stillHeld()) {
                Log.error("leader lease lost (lock session gone), stepping down");
                return;
            }
            cycleAll();
            Thread.sleep(config.cycleIntervalSeconds() * 1000);
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
        lastLakeStats.clear();
        copyAnnounced.clear();
        premakeSkipAnnounced.clear();
        lease.release();
    }

    private void cycleAll() {
        long started = System.nanoTime();
        try {
            java.util.List<RegisteredTable> tables = catalog.listTables();
            for (RegisteredTable table : tables) {
                cycleOne(table);
            }
            statusSweep.run(tables);
            metrics.gauge("tierdb_last_cycle_timestamp_seconds",
                    System.currentTimeMillis() / 1000.0);
        } catch (Exception e) {
            Log.error("cycle sweep failed: %s", e);
        }
        metrics.gauge("tierdb_cycle_duration_seconds", (System.nanoTime() - started) / 1e9);
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

            if (table.mode() == TableMode.DIRECT) {
                try (LakeCommitLock lock = LakeCommitLock.acquire(dataSource, table.id())) {
                    lakeCycle(table, name);
                }
            } else {
                lakeCycle(table, name);
            }

            Cutline cut = catalog.readCutline(table.id());
            if (!cut.equals(lastLogged.put(table.id(), cut))) {
                Log.info("%s: cutline now T=%d S=%d",
                        name, cut.t().value(), cut.snapshot().id());
            }

            maintainIfDue(table);
            collectLakeStatsIfDue(table);
        } catch (Exception e) {
            Log.error("%s: cycle failed (will retry next interval): %s", name, e);
            e.printStackTrace();
        }
    }

    private void lakeCycle(RegisteredTable table, String name) throws Exception {
        LakeStorage lake = lakes.forTable(table);

        try {
            new TieringWorker(catalog, lake, new JdbcHotSource(dataSource),
                    new LagBasedTieringPolicy(dataSource, catalog,
                            table.tierKeyType().parseLagOrWidth(config.tieringLag())),
                    CeilingLagEvictionPolicy.forJdbc(dataSource, table,
                            table.tierKeyType().parseLagOrWidth(config.reclaimLag())))
                    .runCycle(table.id(), Instant.now());
        } catch (ReclaimException e) {
            Log.error("%s: reclaim failed (data is safe; DROP retries next cycle): %s",
                    name, e);
        }

        if (table.mode() != TableMode.DIRECT) {
            // Direct tables have no delta buffer to fold.
            new CompactionWorker(catalog, lake,
                    new JdbcCompactionPolicy(dataSource, catalog, config.compactionBatchSize()))
                    .runCycle(table.id(), Instant.now());
        }

        new LoadAdoptionWorker(catalog, lake).runCycle(table);

        new RetentionWorker(catalog, lake).runCycle(table);
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
                    Log.info("%s: no range partitions, premake skipped", name);
                }
                return;
            }
            if (result.get().outsideGrid()) {
                Log.error("%s: rows sit at or past the top range partition bound "
                        + "(a DEFAULT partition?), premake cannot extend the grid past them",
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
        boolean forced = catalog.consumeMaintenanceRequest(table.id());
        long now = System.currentTimeMillis();
        Long last = lastMaintenance.get(table.id());
        if (!forced && last != null && now - last < config.maintenanceIntervalSeconds() * 1000) {
            return;
        }

        lastMaintenance.put(table.id(), now);
        String name = table.schemaName() + "." + table.tableName();

        try {
            MaintenanceWorker worker = new MaintenanceWorker(catalog,
                    lakes.forTable(table), maintenanceEngine,
                    config.defaultMaintenanceSettings());
            if (forced) {
                Log.info("%s: maintenance pass requested manually", name);
            }

            io.tierdb.lake.maintain.MaintenanceResult result;
            if (table.mode() == TableMode.DIRECT) {
                try (LakeCommitLock lock = LakeCommitLock.acquire(dataSource, table.id())) {
                    result = worker.runCycle(table, forced);
                }
            } else {
                result = worker.runCycle(table, forced);
            }

            if (!result.isNoop()) {
                StringBuilder did = new StringBuilder();
                result.counters().forEach((key, value) ->
                        did.append(' ').append(key).append('=').append(value));
                Log.info("%s: maintenance did%s", name, did);
            }

            int cleaned = stagedFileJanitor.run(table, lakeTableFor(table),
                    worker.buildPlan(table).settings());
            if (cleaned > 0) {
                Log.info("%s: deleted %d staged file(s) of failed loads", name, cleaned);
            }

            collectLakeStats(table);
        } catch (Exception e) {
            Log.error("%s: maintenance failed (will retry next due time): %s", name, e);
        }
    }

    private void collectLakeStatsIfDue(RegisteredTable table) {
        long now = System.currentTimeMillis();
        Long last = lastLakeStats.get(table.id());
        if (last != null && now - last < config.lakeStatsIntervalSeconds() * 1000) {
            return;
        }
        try {
            collectLakeStats(table);
        } catch (Exception e) {
            Log.error("%s.%s: lake stats collection failed (will retry): %s",
                    table.schemaName(), table.tableName(), e);
        }
    }

    private void collectLakeStats(RegisteredTable table) throws Exception {
        lastLakeStats.put(table.id(), System.currentTimeMillis());
        lakeStatsCollector.record(table.id().oid(),
                table.schemaName() + "." + table.tableName(),
                lakeTableFor(table).stats(),
                table.maintenancePolicy().resolve(config.defaultMaintenanceSettings()));
    }

    private LakeTable lakeTableFor(RegisteredTable table) {
        return lakes.forTable(table).table(
                new CommitterInitContext(table.id(), table.lakeTableRef()),
                new ColdTableSpec(table.primaryKeyCols(), table.tierKeyCol()));
    }

    private void cycleMirrored(RegisteredTable table) {
        String name = table.schemaName() + "." + table.tableName();
        try {
            if (catalog.readMirrorFrontier(table.id()).isEmpty()) {
                if (copyAnnounced.add(table.id())) {
                    Log.info("%s: initial copy in progress, mirror cycles wait for it", name);
                }
                return;
            }
            copyAnnounced.remove(table.id());
            MirrorPump pump = mirrorPumps.get(table.id());
            if (pump != null && !pump.thread().isAlive() && pump.worker().diverged()) {
                return;
            }
            if (pump == null || !pump.thread().isAlive()) {
                MirrorWorker.Settings settings = MirrorWorker.Settings.fromConfig(config);
                if (table.heapRetentionLag().isPresent()) {
                    settings = settings.withDeltaFold(
                            new CompactionWorker(catalog, lakes.forTable(table),
                                    new JdbcCompactionPolicy(dataSource, catalog,
                                            config.compactionBatchSize())),
                            config.cycleIntervalSeconds() * 1000L);
                }
                MirrorWorker worker = new MirrorWorker(catalog, lakes.forTable(table),
                        table, settings);
                Thread t = new Thread(worker, "tierdb-mirror-" + name);
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
                new LoadAdoptionWorker(catalog, lakes.forTable(table)).runCycle(table);
            }

            maintainIfDue(table);
            collectLakeStatsIfDue(table);
        } catch (Exception e) {
            Log.error("%s: mirrored cycle failed (will retry next interval): %s", name, e);
        }
    }

}
