package io.modak.worker;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * All environment-specific wiring for a worker process, read from env vars. The
 * same binary points at any deployment shape — local Postgres + filesystem
 * warehouse, Docker + MinIO, or managed Postgres + real S3 — purely by env.
 */
public record WorkerConfig(
        String pgUrl,
        String pgUser,
        String pgPassword,
        String warehouse,
        Map<String, String> lakeConfig,
        long cycleIntervalSeconds,
        long tieringLag,
        long reclaimLag,
        int compactionBatchSize,
        int mirrorBatchRows,
        long mirrorFlushMillis,
        long campaignIntervalSeconds,
        long maintenanceIntervalSeconds,
        long rewriteTargetBytes,
        int rewriteMinInputFiles,
        long snapshotRetentionHours,
        int snapshotMinRetained,
        long slotWarnBytes,
        int metricsPort,
        int mirrorMaxBufferedRows,
        long deltaBacklogWarnRows,
        boolean consoleSql,
        int premakePartitions) {

    public WorkerConfig {
        Map<String, String> merged = new HashMap<>(lakeConfig);
        merged.putIfAbsent("warehouse", warehouse);
        lakeConfig = Map.copyOf(merged);
    }

    /** Core knobs only; maintenance/observability settings take their defaults. */
    public WorkerConfig(String pgUrl, String pgUser, String pgPassword, String warehouse,
            Map<String, String> lakeConfig, long cycleIntervalSeconds, long tieringLag,
            long reclaimLag, int compactionBatchSize, int mirrorBatchRows,
            long mirrorFlushMillis, long campaignIntervalSeconds) {
        this(pgUrl, pgUser, pgPassword, warehouse, lakeConfig, cycleIntervalSeconds,
                tieringLag, reclaimLag, compactionBatchSize, mirrorBatchRows,
                mirrorFlushMillis, campaignIntervalSeconds,
                3600, 128L * 1024 * 1024, 8, 24, 5, 1024L * 1024 * 1024, 0,
                100_000, 100_000, true, 2);
    }

    public static WorkerConfig fromEnv(Map<String, String> env) {
        Map<String, String> lake = new HashMap<>();
        putIfSet(lake, "s3.endpoint", env.get("MODAK_S3_ENDPOINT"));
        putIfSet(lake, "s3.access-key", env.get("MODAK_S3_ACCESS_KEY"));
        putIfSet(lake, "s3.secret-key", env.get("MODAK_S3_SECRET_KEY"));
        putIfSet(lake, "s3.region", env.get("MODAK_S3_REGION"));
        putIfSet(lake, "s3.ssl-enabled", env.get("MODAK_S3_SSL"));
        putIfSet(lake, "catalog.uri", env.get("MODAK_CATALOG_URI"));
        putIfSet(lake, "catalog.warehouse", env.get("MODAK_CATALOG_WAREHOUSE"));
        putIfSet(lake, "catalog.token", env.get("MODAK_CATALOG_TOKEN"));
        putIfSet(lake, "catalog.namespace", env.get("MODAK_CATALOG_NAMESPACE"));
        putIfSet(lake, "format", env.get("MODAK_LAKE_FORMAT"));
        lake.putAll(parseLakeProps(env.get("MODAK_LAKE_PROPS")));

        return new WorkerConfig(
                env.getOrDefault("MODAK_PG_URL", "jdbc:postgresql://localhost:5432/postgres"),
                env.getOrDefault("MODAK_PG_USER", "postgres"),
                env.getOrDefault("MODAK_PG_PASSWORD", ""),
                env.getOrDefault("MODAK_WAREHOUSE", "/tmp/modak-warehouse"),
                Map.copyOf(lake),
                Long.parseLong(env.getOrDefault("MODAK_CYCLE_INTERVAL_SECONDS", "10")),
                Long.parseLong(env.getOrDefault("MODAK_TIERING_LAG", "0")),
                Long.parseLong(env.getOrDefault("MODAK_RECLAIM_LAG",
                        env.getOrDefault("MODAK_TIERING_LAG", "0"))),
                Integer.parseInt(env.getOrDefault("MODAK_COMPACTION_BATCH", "1000")),
                Integer.parseInt(env.getOrDefault("MODAK_MIRROR_BATCH", "500")),
                Long.parseLong(env.getOrDefault("MODAK_MIRROR_FLUSH_MILLIS", "2000")),
                Long.parseLong(env.getOrDefault("MODAK_CAMPAIGN_INTERVAL_SECONDS", "5")),
                Long.parseLong(env.getOrDefault("MODAK_MAINTENANCE_INTERVAL_SECONDS", "3600")),
                Long.parseLong(env.getOrDefault("MODAK_REWRITE_TARGET_BYTES",
                        Long.toString(128L * 1024 * 1024))),
                Integer.parseInt(env.getOrDefault("MODAK_REWRITE_MIN_INPUT_FILES", "8")),
                Long.parseLong(env.getOrDefault("MODAK_SNAPSHOT_RETENTION_HOURS", "24")),
                Integer.parseInt(env.getOrDefault("MODAK_SNAPSHOT_MIN_RETAINED", "5")),
                Long.parseLong(env.getOrDefault("MODAK_SLOT_WARN_BYTES",
                        Long.toString(1024L * 1024 * 1024))),
                Integer.parseInt(env.getOrDefault("MODAK_METRICS_PORT", "0")),
                Integer.parseInt(env.getOrDefault("MODAK_MIRROR_MAX_BUFFERED_ROWS", "100000")),
                Long.parseLong(env.getOrDefault("MODAK_DELTA_BACKLOG_WARN_ROWS", "100000")),
                Boolean.parseBoolean(env.getOrDefault("MODAK_CONSOLE_SQL", "true")),
                Integer.parseInt(env.getOrDefault("MODAK_PREMAKE_PARTITIONS", "2")));
    }

    /** Same config with the worker's own metrics endpoint moved/disabled. */
    public WorkerConfig withMetricsPort(int port) {
        return new WorkerConfig(pgUrl, pgUser, pgPassword, warehouse, lakeConfig,
                cycleIntervalSeconds, tieringLag, reclaimLag, compactionBatchSize,
                mirrorBatchRows, mirrorFlushMillis, campaignIntervalSeconds,
                maintenanceIntervalSeconds, rewriteTargetBytes, rewriteMinInputFiles,
                snapshotRetentionHours, snapshotMinRetained, slotWarnBytes, port,
                mirrorMaxBufferedRows, deltaBacklogWarnRows, consoleSql, premakePartitions);
    }

    public String lakeFormat() {
        return lakeConfig.getOrDefault("format", "iceberg");
    }

    /** Parses {@code MODAK_LAKE_PROPS}: semicolon-separated {@code key=value} pairs. */
    static Map<String, String> parseLakeProps(String props) {
        Map<String, String> out = new HashMap<>();
        if (props == null || props.isBlank()) {
            return out;
        }
        for (String pair : props.split(";")) {
            if (pair.isBlank()) {
                continue;
            }
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "MODAK_LAKE_PROPS entries must be key=value: '" + pair + "'");
            }
            out.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return out;
    }

    public io.modak.lake.MaintenanceConfig maintenanceConfig() {
        return new io.modak.lake.MaintenanceConfig(rewriteTargetBytes, rewriteMinInputFiles,
                snapshotRetentionHours * 3_600_000L, snapshotMinRetained);
    }

    public DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(pgUrl);
        ds.setUser(pgUser);
        ds.setPassword(pgPassword);
        // Workers reason about the physical heap; transparent reads must not widen their queries.
        ds.setOptions("-c modak.transparent_reads=off");
        return ds;
    }

    /** Console playground connections: transparent reads stay ON, as a user sees them. */
    public DataSource consoleDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(pgUrl);
        ds.setUser(pgUser);
        ds.setPassword(pgPassword);
        return ds;
    }

    private static void putIfSet(Map<String, String> map, String key, String value) {
        if (value != null && !value.isBlank()) {
            map.put(key, value);
        }
    }
}
