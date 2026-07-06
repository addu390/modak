package io.tierdb.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSchemaTest {

    private EmbeddedPostgres postgres;
    private DataSource dataSource;

    @BeforeEach
    void setUp() throws IOException {
        postgres = EmbeddedPostgres.start();
        dataSource = postgres.getPostgresDatabase();
    }

    @AfterEach
    void tearDown() throws IOException {
        postgres.close();
    }

    @Test
    void aFreshDatabaseGetsTheBaselineStampedAtTheCurrentVersion() {
        CatalogSchema.apply(dataSource);
        assertEquals(CatalogSchema.CURRENT_VERSION, installedVersion());
        assertTrue(Boolean.parseBoolean(queryOne(
                "SELECT (to_regclass('tierdb.copy_progress') IS NOT NULL)::text")),
                "the baseline carries the newest tables");
    }

    @Test
    void applyIsIdempotentOnAnUpToDateDatabase() {
        CatalogSchema.apply(dataSource);
        CatalogSchema.apply(dataSource);
        assertEquals(CatalogSchema.CURRENT_VERSION, installedVersion());
    }

    @Test
    void anUnstampedSchemaIsStampedAtTheCurrentVersion() {
        CatalogSchema.apply(dataSource);
        exec("DROP TABLE tierdb.schema_meta");

        CatalogSchema.apply(dataSource);

        assertEquals(CatalogSchema.CURRENT_VERSION, installedVersion());
    }

    @Test
    void aNewerSchemaThanTheWorkerRefusesToApply() {
        CatalogSchema.apply(dataSource);
        exec("UPDATE tierdb.schema_meta SET version = " + (CatalogSchema.CURRENT_VERSION + 1));
        CatalogException e = assertThrows(CatalogException.class,
                () -> CatalogSchema.apply(dataSource));
        assertTrue(e.getMessage().contains("upgrade the worker"),
                "refusal names the fix: " + e);
    }

    private int installedVersion() {
        return Integer.parseInt(queryOne("SELECT version::text FROM tierdb.schema_meta"));
    }

    private void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String queryOne(String sql) {
        try (Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
