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

    private SeamOptions(Builder b) {
        this.jdbcUrl = Objects.requireNonNull(b.jdbcUrl, "jdbcUrl");
        this.jdbcProperties = b.jdbcProperties;
        Objects.requireNonNull(b.table, "table");
        int dot = b.table.indexOf('.');
        this.schemaName = dot < 0 ? "public" : b.table.substring(0, dot);
        this.tableName = dot < 0 ? b.table : b.table.substring(dot + 1);
        this.pinTtl = b.pinTtl;
        this.lakeTable = b.lakeTable;
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

    public static final class Builder {

        private String jdbcUrl;
        private final Properties jdbcProperties = new Properties();
        private String table;
        private Duration pinTtl = Duration.ofMinutes(15);
        private String lakeTable;

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

        public SeamOptions build() {
            return new SeamOptions(this);
        }
    }
}
