package io.modak.worker;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * All environment-specific wiring for a worker process, read from env vars.
 * The same binary points at any deployment shape purely by env, whether local
 * Postgres with a filesystem warehouse, Docker with MinIO, or managed Postgres with S3.
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
        int premakePartitions,
        String loadToken,
        int loadSpoolThreshold) {

    public WorkerConfig {
        Map<String, String> merged = new HashMap<>(lakeConfig);
        merged.putIfAbsent("warehouse", warehouse);
        lakeConfig = Map.copyOf(merged);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Every knob defaults to its {@code fromEnv} default, set only what differs. */
    public static final class Builder {
        private String pgUrl = "jdbc:postgresql://localhost:5432/postgres";
        private String pgUser = "postgres";
        private String pgPassword = "";
        private String warehouse = "/tmp/modak-warehouse";
        private Map<String, String> lakeConfig = Map.of();
        private long cycleIntervalSeconds = 10;
        private long tieringLag = 0;
        private long reclaimLag = 0;
        private int compactionBatchSize = 1000;
        private int mirrorBatchRows = 500;
        private long mirrorFlushMillis = 2000;
        private long campaignIntervalSeconds = 5;
        private long maintenanceIntervalSeconds = 3600;
        private long rewriteTargetBytes = 128L * 1024 * 1024;
        private int rewriteMinInputFiles = 8;
        private long snapshotRetentionHours = 24;
        private int snapshotMinRetained = 5;
        private long slotWarnBytes = 1024L * 1024 * 1024;
        private int metricsPort = 0;
        private int mirrorMaxBufferedRows = 100_000;
        private long deltaBacklogWarnRows = 100_000;
        private boolean consoleSql = true;
        private int premakePartitions = 2;
        private String loadToken;
        private int loadSpoolThreshold = 1000;

        public Builder pgUrl(String v) { pgUrl = v; return this; }
        public Builder pgUser(String v) { pgUser = v; return this; }
        public Builder pgPassword(String v) { pgPassword = v; return this; }
        public Builder warehouse(String v) { warehouse = v; return this; }
        public Builder lakeConfig(Map<String, String> v) { lakeConfig = v; return this; }
        public Builder cycleIntervalSeconds(long v) { cycleIntervalSeconds = v; return this; }
        public Builder tieringLag(long v) { tieringLag = v; return this; }
        public Builder reclaimLag(long v) { reclaimLag = v; return this; }
        public Builder compactionBatchSize(int v) { compactionBatchSize = v; return this; }
        public Builder mirrorBatchRows(int v) { mirrorBatchRows = v; return this; }
        public Builder mirrorFlushMillis(long v) { mirrorFlushMillis = v; return this; }
        public Builder campaignIntervalSeconds(long v) { campaignIntervalSeconds = v; return this; }
        public Builder maintenanceIntervalSeconds(long v) { maintenanceIntervalSeconds = v; return this; }
        public Builder rewriteTargetBytes(long v) { rewriteTargetBytes = v; return this; }
        public Builder rewriteMinInputFiles(int v) { rewriteMinInputFiles = v; return this; }
        public Builder snapshotRetentionHours(long v) { snapshotRetentionHours = v; return this; }
        public Builder snapshotMinRetained(int v) { snapshotMinRetained = v; return this; }
        public Builder slotWarnBytes(long v) { slotWarnBytes = v; return this; }
        public Builder metricsPort(int v) { metricsPort = v; return this; }
        public Builder mirrorMaxBufferedRows(int v) { mirrorMaxBufferedRows = v; return this; }
        public Builder deltaBacklogWarnRows(long v) { deltaBacklogWarnRows = v; return this; }
        public Builder consoleSql(boolean v) { consoleSql = v; return this; }
        public Builder premakePartitions(int v) { premakePartitions = v; return this; }
        public Builder loadToken(String v) { loadToken = v; return this; }
        public Builder loadSpoolThreshold(int v) { loadSpoolThreshold = v; return this; }

        public WorkerConfig build() {
            return new WorkerConfig(pgUrl, pgUser, pgPassword, warehouse, lakeConfig,
                    cycleIntervalSeconds, tieringLag, reclaimLag, compactionBatchSize,
                    mirrorBatchRows, mirrorFlushMillis, campaignIntervalSeconds,
                    maintenanceIntervalSeconds, rewriteTargetBytes, rewriteMinInputFiles,
                    snapshotRetentionHours, snapshotMinRetained, slotWarnBytes, metricsPort,
                    mirrorMaxBufferedRows, deltaBacklogWarnRows, consoleSql, premakePartitions,
                    loadToken, loadSpoolThreshold);
        }
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

        Builder b = builder()
                .lakeConfig(Map.copyOf(lake))
                .loadToken(blankToNull(env.get("MODAK_LOAD_TOKEN")));
        ifSet(env, "MODAK_PG_URL", b::pgUrl);
        ifSet(env, "MODAK_PG_USER", b::pgUser);
        ifSet(env, "MODAK_PG_PASSWORD", b::pgPassword);
        ifSet(env, "MODAK_WAREHOUSE", b::warehouse);
        ifSetLong(env, "MODAK_CYCLE_INTERVAL_SECONDS", b::cycleIntervalSeconds);
        ifSetLong(env, "MODAK_TIERING_LAG", b::tieringLag);
        b.reclaimLag(Long.parseLong(env.getOrDefault("MODAK_RECLAIM_LAG",
                env.getOrDefault("MODAK_TIERING_LAG", "0"))));
        ifSetInt(env, "MODAK_COMPACTION_BATCH", b::compactionBatchSize);
        ifSetInt(env, "MODAK_MIRROR_BATCH", b::mirrorBatchRows);
        ifSetLong(env, "MODAK_MIRROR_FLUSH_MILLIS", b::mirrorFlushMillis);
        ifSetLong(env, "MODAK_CAMPAIGN_INTERVAL_SECONDS", b::campaignIntervalSeconds);
        ifSetLong(env, "MODAK_MAINTENANCE_INTERVAL_SECONDS", b::maintenanceIntervalSeconds);
        ifSetLong(env, "MODAK_REWRITE_TARGET_BYTES", b::rewriteTargetBytes);
        ifSetInt(env, "MODAK_REWRITE_MIN_INPUT_FILES", b::rewriteMinInputFiles);
        ifSetLong(env, "MODAK_SNAPSHOT_RETENTION_HOURS", b::snapshotRetentionHours);
        ifSetInt(env, "MODAK_SNAPSHOT_MIN_RETAINED", b::snapshotMinRetained);
        ifSetLong(env, "MODAK_SLOT_WARN_BYTES", b::slotWarnBytes);
        ifSetInt(env, "MODAK_METRICS_PORT", b::metricsPort);
        ifSetInt(env, "MODAK_MIRROR_MAX_BUFFERED_ROWS", b::mirrorMaxBufferedRows);
        ifSetLong(env, "MODAK_DELTA_BACKLOG_WARN_ROWS", b::deltaBacklogWarnRows);
        ifSet(env, "MODAK_CONSOLE_SQL", v -> b.consoleSql(Boolean.parseBoolean(v)));
        ifSetInt(env, "MODAK_PREMAKE_PARTITIONS", b::premakePartitions);
        ifSetInt(env, "MODAK_LOAD_SPOOL_THRESHOLD", b::loadSpoolThreshold);
        return b.build();
    }

    /** Same config with the worker's own metrics endpoint moved/disabled. */
    public WorkerConfig withMetricsPort(int port) {
        return new WorkerConfig(pgUrl, pgUser, pgPassword, warehouse, lakeConfig,
                cycleIntervalSeconds, tieringLag, reclaimLag, compactionBatchSize,
                mirrorBatchRows, mirrorFlushMillis, campaignIntervalSeconds,
                maintenanceIntervalSeconds, rewriteTargetBytes, rewriteMinInputFiles,
                snapshotRetentionHours, snapshotMinRetained, slotWarnBytes, port,
                mirrorMaxBufferedRows, deltaBacklogWarnRows, consoleSql, premakePartitions,
                loadToken, loadSpoolThreshold);
    }

    private static void ifSet(Map<String, String> env, String key,
            java.util.function.Consumer<String> setter) {
        String value = env.get(key);
        if (value != null) {
            setter.accept(value);
        }
    }

    private static void ifSetLong(Map<String, String> env, String key,
            java.util.function.LongConsumer setter) {
        ifSet(env, key, v -> setter.accept(Long.parseLong(v)));
    }

    private static void ifSetInt(Map<String, String> env, String key,
            java.util.function.IntConsumer setter) {
        ifSet(env, key, v -> setter.accept(Integer.parseInt(v)));
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
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
        // Workers reason about the physical heap, transparent reads must not widen their queries.
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
