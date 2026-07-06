package io.tierdb.catalog;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Versioned {@code tierdb.*} schema management: a fresh database gets the
 * full {@code /tierdb/catalog.sql} baseline, an older one gets the pending
 * {@code /tierdb/migrations/V*.sql} in order.
 */
public final class CatalogSchema {

    public static final String RESOURCE = "/tierdb/catalog.sql";
    public static final int CURRENT_VERSION = 1;

    private static final int LEGACY_VERSION = 1;

    private static final Map<Integer, String> MIGRATIONS = Map.of();

    private static final long MIGRATION_LOCK_KEY = 0x6d6f64616b31L;

    private CatalogSchema() {}

    public static void apply(DataSource dataSource) {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("SELECT pg_advisory_xact_lock(" + MIGRATION_LOCK_KEY + ")");
                int installed = installedVersion(s);
                if (installed < 0) {
                    s.execute(readResource());
                } else if (installed > CURRENT_VERSION) {
                    throw new CatalogException("catalog schema is v" + installed
                            + " but this worker only knows v" + CURRENT_VERSION
                            + ", upgrade the worker before pointing it at this database");
                } else {
                    for (int v = installed + 1; v <= CURRENT_VERSION; v++) {
                        s.execute(read(MIGRATIONS.get(v)));
                    }
                    stamp(s);
                }
            }
            c.commit();
        } catch (SQLException e) {
            throw new CatalogException("failed to apply the tierdb catalog schema", e);
        }
    }

    private static int installedVersion(Statement s) throws SQLException {
        try (ResultSet rs = s.executeQuery(
                "SELECT 1 FROM pg_namespace WHERE nspname = 'tierdb'")) {
            if (!rs.next()) {
                return -1;
            }
        }
        try (ResultSet rs = s.executeQuery("SELECT to_regclass('tierdb.schema_meta')::text")) {
            rs.next();
            if (rs.getString(1) == null) {
                return LEGACY_VERSION;
            }
        }
        try (ResultSet rs = s.executeQuery("SELECT max(version) FROM tierdb.schema_meta")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static void stamp(Statement s) throws SQLException {
        s.execute("""
                CREATE TABLE IF NOT EXISTS tierdb.schema_meta (
                    version int NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());
                INSERT INTO tierdb.schema_meta (version)
                SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM tierdb.schema_meta);
                UPDATE tierdb.schema_meta SET version = %d, applied_at = now();
                """.formatted(CURRENT_VERSION));
    }

    public static String readResource() {
        return read(RESOURCE);
    }

    private static String read(String resource) {
        try (InputStream in = CatalogSchema.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new CatalogException("catalog DDL not found on classpath: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new CatalogException("failed to read " + resource, e);
        }
    }
}
