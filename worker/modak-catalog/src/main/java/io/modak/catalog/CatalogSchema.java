package io.modak.catalog;

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
 * Versioned {@code modak.*} schema management: a fresh database gets the full
 * {@code /modak/catalog.sql} baseline (always the latest shape), an older one
 * gets the pending {@code /modak/migrations/V*.sql} in order, and a NEWER one
 * makes this (older) worker refuse to run. Serialized by an advisory lock.
 */
public final class CatalogSchema {

    public static final String RESOURCE = "/modak/catalog.sql";
    public static final int CURRENT_VERSION = 1;

    // No schema_meta = V1 baseline.
    private static final int LEGACY_VERSION = 1;

    // Post-release schema changes add /modak/migrations/V*.sql here and bump the version.
    private static final Map<Integer, String> MIGRATIONS = Map.of();

    private static final long MIGRATION_LOCK_KEY = 0x6d6f64616b31L; // distinct from the leader lease

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
            throw new CatalogException("failed to apply the modak catalog schema", e);
        }
    }

    /** -1 = no modak schema at all, {@link #LEGACY_VERSION} = schema without a stamp. */
    private static int installedVersion(Statement s) throws SQLException {
        try (ResultSet rs = s.executeQuery(
                "SELECT 1 FROM pg_namespace WHERE nspname = 'modak'")) {
            if (!rs.next()) {
                return -1;
            }
        }
        try (ResultSet rs = s.executeQuery("SELECT to_regclass('modak.schema_meta')::text")) {
            rs.next();
            if (rs.getString(1) == null) {
                return LEGACY_VERSION;
            }
        }
        try (ResultSet rs = s.executeQuery("SELECT max(version) FROM modak.schema_meta")) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private static void stamp(Statement s) throws SQLException {
        s.execute("""
                CREATE TABLE IF NOT EXISTS modak.schema_meta (
                    version int NOT NULL, applied_at timestamptz NOT NULL DEFAULT now());
                INSERT INTO modak.schema_meta (version)
                SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM modak.schema_meta);
                UPDATE modak.schema_meta SET version = %d, applied_at = now();
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
