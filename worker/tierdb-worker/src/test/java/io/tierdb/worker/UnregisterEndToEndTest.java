package io.tierdb.worker;

import io.tierdb.worker.cli.TableRegistrar;
import io.tierdb.worker.cli.TableUnregistrar;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class UnregisterEndToEndTest {

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static WorkerConfig config;

    @BeforeAll
    static void setUpWorld() throws Exception {
        postgres = EmbeddedPostgres.builder()
                .setServerConfig("wal_level", "logical")
                .start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);
        config = WorkerConfig.builder()
                .pgUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .warehouse(warehouse.toString())
                .mirrorFlushMillis(200).campaignIntervalSeconds(1).build();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void unregisterRemovesCatalogRowSlotAndPublicationButKeepsTheLake() throws Exception {
        exec("CREATE TABLE public.fleets (id bigint PRIMARY KEY, name text, seen bigint NOT NULL)");
        exec("INSERT INTO public.fleets VALUES (1, 'alpha', 10)");
        TableRegistrar.run(config, new String[] {
            "--table", "public.fleets", "--pk", "id", "--tier-key", "seen",
            "--mode", "mirrored",
        });
        assertEquals("1", queryOne("SELECT count(*)::text FROM tierdb.tables "
                + "WHERE schema_name = 'public' AND table_name = 'fleets'"));
        assertEquals("1", queryOne("SELECT count(*)::text FROM pg_replication_slots "
                + "WHERE slot_name = 'tierdb_slot_public_fleets'"));

        TableUnregistrar.run(config, new String[] {"--table", "public.fleets"});

        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.tables "
                + "WHERE schema_name = 'public' AND table_name = 'fleets'"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.cutline c "
                + "JOIN tierdb.tables t USING (table_id) WHERE t.table_name = 'fleets'"),
                "cutline cascaded");
        assertEquals("0", queryOne("SELECT count(*)::text FROM pg_replication_slots "
                + "WHERE slot_name = 'tierdb_slot_public_fleets'"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM pg_publication "
                + "WHERE pubname = 'tierdb_pub_public_fleets'"));
        assertEquals("d", queryOne("SELECT relreplident::text FROM pg_class "
                + "WHERE oid = 'public.fleets'::regclass"), "replica identity reset");
        assertTrue(new HadoopTables(new Configuration())
                        .exists(warehouse + "/public.fleets"),
                "lake table kept without --drop-lake");
    }

    @Test
    void dropLakePurgesTheColdTable() throws Exception {
        exec("CREATE TABLE public.depots (id bigint PRIMARY KEY, seen bigint NOT NULL)");
        TableRegistrar.run(config, new String[] {
            "--table", "public.depots", "--pk", "id", "--tier-key", "seen",
            "--mode", "mirrored",
        });
        assertTrue(new HadoopTables(new Configuration()).exists(warehouse + "/public.depots"));

        TableUnregistrar.run(config, new String[] {"--table", "public.depots", "--drop-lake"});

        assertFalse(new HadoopTables(new Configuration()).exists(warehouse + "/public.depots"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.tables "
                + "WHERE table_name = 'depots'"));
    }

    @Test
    void unknownTableIsANoopCleanup() throws Exception {
        exec("CREATE TABLE public.ghosts (id bigint PRIMARY KEY, seen bigint NOT NULL)");
        TableUnregistrar.run(config, new String[] {"--table", "public.ghosts"});
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.tables "
                + "WHERE table_name = 'ghosts'"));
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
