package io.tierdb.worker;

import io.tierdb.worker.cli.TableRegistrar;
import io.tierdb.worker.cli.TableUnregistrar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The {@code --keep-heap} flag: validation, the stamped catalog row, and the
 * unregister-time trigger cleanup.
 */
class KeepHeapRegistrarTest {

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static WorkerConfig config;

    @BeforeAll
    static void setUpWorld() throws Exception {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);
        config = WorkerConfig.builder()
                .pgUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .warehouse(warehouse.toString())
                .build();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void keepHeapRegistersTieredWithTheFlagStamped() throws Exception {
        exec("CREATE TABLE public.readings (id bigint NOT NULL, ts bigint NOT NULL) "
                + "PARTITION BY RANGE (ts)");
        exec("CREATE TABLE public.readings_p0 PARTITION OF public.readings "
                + "FOR VALUES FROM (0) TO (100)");

        TableRegistrar.run(config, new String[] {
            "--table", "public.readings", "--pk", "id", "--tier-key", "ts", "--keep-heap",
        });

        RegisteredTable meta = new JdbcCatalog(dataSource)
                .lookup("public", "readings").orElseThrow();
        assertTrue(meta.keepHeap());
        assertFalse(meta.dropsHeapPartitions());
    }

    @Test
    void keepHeapIsRejectedForMirroredMode() {
        exec("CREATE TABLE public.fleets (id bigint PRIMARY KEY, seen bigint NOT NULL)");
        assertThrows(IllegalArgumentException.class, () -> TableRegistrar.run(config,
                new String[] {
                    "--table", "public.fleets", "--pk", "id", "--tier-key", "seen",
                    "--mode", "mirrored", "--keep-heap",
                }));
    }

    @Test
    void keepHeapExcludesLakeRetention() {
        exec("CREATE TABLE public.metrics (id bigint NOT NULL, ts bigint NOT NULL) "
                + "PARTITION BY RANGE (ts)");
        assertThrows(IllegalArgumentException.class, () -> TableRegistrar.run(config,
                new String[] {
                    "--table", "public.metrics", "--pk", "id", "--tier-key", "ts",
                    "--keep-heap", "--lake-retention", "100",
                }));
    }

    @Test
    void unregisterDropsTheColdMirrorTriggers() throws Exception {
        exec("CREATE TABLE public.samples (id bigint NOT NULL, ts bigint NOT NULL) "
                + "PARTITION BY RANGE (ts)");
        exec("CREATE TABLE public.samples_p0 PARTITION OF public.samples "
                + "FOR VALUES FROM (0) TO (100)");
        TableRegistrar.run(config, new String[] {
            "--table", "public.samples", "--pk", "id", "--tier-key", "ts", "--keep-heap",
        });

        exec("CREATE FUNCTION public.noop_tg() RETURNS trigger LANGUAGE plpgsql "
                + "AS $$BEGIN RETURN NULL; END$$");
        exec("CREATE TRIGGER tierdb_cold_mirror AFTER INSERT ON public.samples_p0 "
                + "FOR EACH ROW EXECUTE FUNCTION public.noop_tg()");
        exec("""
                CREATE FUNCTION tierdb_detach_cold_mirror(t oid) RETURNS bigint
                LANGUAGE plpgsql AS $body$
                DECLARE r record; n bigint := 0;
                BEGIN
                    FOR r IN SELECT i.inhrelid::regclass AS part FROM pg_inherits i
                               JOIN pg_trigger g ON g.tgrelid = i.inhrelid
                              WHERE i.inhparent = t AND g.tgname = 'tierdb_cold_mirror' LOOP
                        EXECUTE format('DROP TRIGGER tierdb_cold_mirror ON %s', r.part);
                        n := n + 1;
                    END LOOP;
                    RETURN n;
                END
                $body$
                """);

        TableUnregistrar.run(config, new String[] {"--table", "public.samples"});

        assertEquals("0", queryOne("SELECT count(*)::text FROM pg_trigger "
                + "WHERE tgname = 'tierdb_cold_mirror'"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.tables "
                + "WHERE table_name = 'samples'"));
    }

    private static void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String queryOne(String sql) {
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
