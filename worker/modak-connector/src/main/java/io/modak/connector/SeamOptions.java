package io.modak.connector;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

/**
 * How to reach a Modak table: the Postgres holding the heap and the catalog,
 * the schema-qualified table name, and the pin lifetime. {@code lakeTable}
 * overrides where the cold branch loads from when the engine resolves the
 * lake table through its own catalog.
 */
public final class SeamOptions {

    private final String jdbcUrl;
    private final Properties jdbcProperties;
    private final String schemaName;
    private final String tableName;
    private final Duration pinTtl;
    private final String lakeTable;
    private final boolean hybrid;
    private final long hybridLag;
    private final Duration mirrorWait;

    private SeamOptions(Builder b) {
        this.jdbcUrl = Objects.requireNonNull(b.jdbcUrl, "jdbcUrl");
        this.jdbcProperties = b.jdbcProperties;
        Objects.requireNonNull(b.table, "table");
        int dot = b.table.indexOf('.');
        this.schemaName = dot < 0 ? "public" : b.table.substring(0, dot);
        this.tableName = dot < 0 ? b.table : b.table.substring(dot + 1);
        this.pinTtl = b.pinTtl;
        this.lakeTable = b.lakeTable;
        this.hybrid = b.hybrid;
        this.hybridLag = b.hybridLag;
        this.mirrorWait = b.mirrorWait;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }

    public Properties jdbcProperties() {
        return jdbcProperties;
    }

    public String schemaName() {
        return schemaName;
    }

    public String tableName() {
        return tableName;
    }

    public String qualifiedName() {
        return schemaName + "." + tableName;
    }

    public Duration pinTtl() {
        return pinTtl;
    }

    public String lakeTable() {
        return lakeTable;
    }

    public boolean hybrid() {
        return hybrid;
    }

    public long hybridLag() {
        return hybridLag;
    }

    public Duration mirrorWait() {
        return mirrorWait;
    }

    public static final class Builder {

        private String jdbcUrl;
        private final Properties jdbcProperties = new Properties();
        private String table;
        private Duration pinTtl = Duration.ofMinutes(15);
        private String lakeTable;
        private boolean hybrid;
        private long hybridLag;
        private Duration mirrorWait = Duration.ofSeconds(5);

        public Builder jdbcUrl(String url) {
            this.jdbcUrl = url;
            return this;
        }

        public Builder jdbcProperty(String key, String value) {
            this.jdbcProperties.setProperty(key, value);
            return this;
        }

        public Builder table(String qualifiedName) {
            this.table = qualifiedName;
            return this;
        }

        public Builder pinTtl(Duration ttl) {
            this.pinTtl = ttl;
            return this;
        }

        public Builder lakeTable(String identifier) {
            this.lakeTable = identifier;
            return this;
        }

        /**
         * Two-tier reads on fully mirrored tables: the bulk of the scan moves to
         * the lake once the mirror frontier provably covers this read. Falls back
         * to the (always correct) heap when the frontier lags past
         * {@code mirrorWait}.
         */
        public Builder hybrid(boolean enabled) {
            this.hybrid = enabled;
            return this;
        }

        /** Tier-key margin kept on the heap side of a hybrid read (default 0). */
        public Builder hybridLag(long lag) {
            this.hybridLag = lag;
            return this;
        }

        /** Bounded wait for the mirror frontier before a hybrid read (default 5s). */
        public Builder mirrorWait(Duration wait) {
            this.mirrorWait = wait;
            return this;
        }

        public SeamOptions build() {
            return new SeamOptions(this);
        }
    }
}
