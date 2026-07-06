package io.tierdb.worker;

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
        String tieringLag,
        String reclaimLag,
        int compactionBatchSize,
        int mirrorBatchRows,
        long mirrorFlushMillis,
        long campaignIntervalSeconds,
        boolean maintenanceEnabled,
        long maintenanceIntervalSeconds,
        String maintenanceEngine,
        long lakeStatsIntervalSeconds,
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
        if (!"embedded".equals(maintenanceEngine)) {
            throw new IllegalArgumentException("unknown TIERDB_MAINTENANCE_ENGINE '"
                    + maintenanceEngine + "', 'embedded' is the only engine so far");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Every knob defaults to its {@code fromEnv} default, set only what differs. */
    public static final class Builder {
        private String pgUrl = "jdbc:postgresql://localhost:5432/postgres";
        private String pgUser = "postgres";
        private String pgPassword = "";
        private String warehouse = "/tmp/tierdb-warehouse";
        private Map<String, String> lakeConfig = Map.of();
        private long cycleIntervalSeconds = 10;
        private String tieringLag = "0";
        private String reclaimLag = "0";
        private int compactionBatchSize = 1000;
        private int mirrorBatchRows = 500;
        private long mirrorFlushMillis = 2000;
        private long campaignIntervalSeconds = 5;
        private boolean maintenanceEnabled = true;
        private long maintenanceIntervalSeconds = 3600;
        private String maintenanceEngine = "embedded";
        private long lakeStatsIntervalSeconds = 60;
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
        public Builder tieringLag(String v) { tieringLag = v; return this; }
        public Builder reclaimLag(String v) { reclaimLag = v; return this; }
        public Builder compactionBatchSize(int v) { compactionBatchSize = v; return this; }
        public Builder mirrorBatchRows(int v) { mirrorBatchRows = v; return this; }
        public Builder mirrorFlushMillis(long v) { mirrorFlushMillis = v; return this; }
        public Builder campaignIntervalSeconds(long v) { campaignIntervalSeconds = v; return this; }
        public Builder maintenanceEnabled(boolean v) { maintenanceEnabled = v; return this; }
        public Builder maintenanceIntervalSeconds(long v) { maintenanceIntervalSeconds = v; return this; }
        public Builder maintenanceEngine(String v) { maintenanceEngine = v; return this; }
        public Builder lakeStatsIntervalSeconds(long v) { lakeStatsIntervalSeconds = v; return this; }
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
                    maintenanceEnabled, maintenanceIntervalSeconds, maintenanceEngine,
                    lakeStatsIntervalSeconds, rewriteTargetBytes, rewriteMinInputFiles,
                    snapshotRetentionHours, snapshotMinRetained, slotWarnBytes, metricsPort,
                    mirrorMaxBufferedRows, deltaBacklogWarnRows, consoleSql, premakePartitions,
                    loadToken, loadSpoolThreshold);
        }
    }

    public static WorkerConfig fromEnv(Map<String, String> env) {
        Map<String, String> lake = new HashMap<>();
        putIfSet(lake, "s3.endpoint", env.get("TIERDB_S3_ENDPOINT"));
        putIfSet(lake, "s3.access-key", env.get("TIERDB_S3_ACCESS_KEY"));
        putIfSet(lake, "s3.secret-key", env.get("TIERDB_S3_SECRET_KEY"));
        putIfSet(lake, "s3.region", env.get("TIERDB_S3_REGION"));
        putIfSet(lake, "s3.ssl-enabled", env.get("TIERDB_S3_SSL"));
        putIfSet(lake, "catalog.uri", env.get("TIERDB_CATALOG_URI"));
        putIfSet(lake, "catalog.warehouse", env.get("TIERDB_CATALOG_WAREHOUSE"));
        putIfSet(lake, "catalog.token", env.get("TIERDB_CATALOG_TOKEN"));
        putIfSet(lake, "catalog.namespace", env.get("TIERDB_CATALOG_NAMESPACE"));
        putIfSet(lake, "format", env.get("TIERDB_LAKE_FORMAT"));
        lake.putAll(parseLakeProps(env.get("TIERDB_LAKE_PROPS")));

        Builder b = builder()
                .lakeConfig(Map.copyOf(lake))
                .loadToken(blankToNull(env.get("TIERDB_LOAD_TOKEN")));
        ifSet(env, "TIERDB_PG_URL", b::pgUrl);
        ifSet(env, "TIERDB_PG_USER", b::pgUser);
        ifSet(env, "TIERDB_PG_PASSWORD", b::pgPassword);
        ifSet(env, "TIERDB_WAREHOUSE", b::warehouse);
        ifSetLong(env, "TIERDB_CYCLE_INTERVAL_SECONDS", b::cycleIntervalSeconds);
        ifSet(env, "TIERDB_TIERING_LAG", b::tieringLag);
        b.reclaimLag(env.getOrDefault("TIERDB_RECLAIM_LAG",
                env.getOrDefault("TIERDB_TIERING_LAG", "0")));
        ifSetInt(env, "TIERDB_COMPACTION_BATCH", b::compactionBatchSize);
        ifSetInt(env, "TIERDB_MIRROR_BATCH", b::mirrorBatchRows);
        ifSetLong(env, "TIERDB_MIRROR_FLUSH_MILLIS", b::mirrorFlushMillis);
        ifSetLong(env, "TIERDB_CAMPAIGN_INTERVAL_SECONDS", b::campaignIntervalSeconds);
        ifSet(env, "TIERDB_MAINTENANCE_ENABLED", v -> b.maintenanceEnabled(Boolean.parseBoolean(v)));
        ifSetLong(env, "TIERDB_MAINTENANCE_INTERVAL_SECONDS", b::maintenanceIntervalSeconds);
        ifSet(env, "TIERDB_MAINTENANCE_ENGINE", b::maintenanceEngine);
        ifSetLong(env, "TIERDB_LAKE_STATS_INTERVAL_SECONDS", b::lakeStatsIntervalSeconds);
        ifSetLong(env, "TIERDB_REWRITE_TARGET_BYTES", b::rewriteTargetBytes);
        ifSetInt(env, "TIERDB_REWRITE_MIN_INPUT_FILES", b::rewriteMinInputFiles);
        ifSetLong(env, "TIERDB_SNAPSHOT_RETENTION_HOURS", b::snapshotRetentionHours);
        ifSetInt(env, "TIERDB_SNAPSHOT_MIN_RETAINED", b::snapshotMinRetained);
        ifSetLong(env, "TIERDB_SLOT_WARN_BYTES", b::slotWarnBytes);
        ifSetInt(env, "TIERDB_METRICS_PORT", b::metricsPort);
        ifSetInt(env, "TIERDB_MIRROR_MAX_BUFFERED_ROWS", b::mirrorMaxBufferedRows);
        ifSetLong(env, "TIERDB_DELTA_BACKLOG_WARN_ROWS", b::deltaBacklogWarnRows);
        ifSet(env, "TIERDB_CONSOLE_SQL", v -> b.consoleSql(Boolean.parseBoolean(v)));
        ifSetInt(env, "TIERDB_PREMAKE_PARTITIONS", b::premakePartitions);
        ifSetInt(env, "TIERDB_LOAD_SPOOL_THRESHOLD", b::loadSpoolThreshold);
        return b.build();
    }

    public WorkerConfig withMetricsPort(int port) {
        return new WorkerConfig(pgUrl, pgUser, pgPassword, warehouse, lakeConfig,
                cycleIntervalSeconds, tieringLag, reclaimLag, compactionBatchSize,
                mirrorBatchRows, mirrorFlushMillis, campaignIntervalSeconds,
                maintenanceEnabled, maintenanceIntervalSeconds, maintenanceEngine,
                lakeStatsIntervalSeconds, rewriteTargetBytes, rewriteMinInputFiles,
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

    public static Map<String, String> parseLakeProps(String props) {
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
                        "lake config entries must be key=value: '" + pair + "'");
            }
            out.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }
        return out;
    }

    public Map<String, String> defaultMaintenanceSettings() {
        return Map.of(
                "maintenance_enabled", String.valueOf(maintenanceEnabled),
                "rewrite_target_bytes", String.valueOf(rewriteTargetBytes),
                "rewrite_min_input_files", String.valueOf(rewriteMinInputFiles),
                "snapshot_retention_hours", String.valueOf(snapshotRetentionHours),
                "snapshot_min_retained", String.valueOf(snapshotMinRetained));
    }

    public DataSource dataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(pgUrl);
        ds.setUser(pgUser);
        ds.setPassword(pgPassword);
        ds.setOptions("-c tierdb.transparent_reads=off");
        return ds;
    }

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
