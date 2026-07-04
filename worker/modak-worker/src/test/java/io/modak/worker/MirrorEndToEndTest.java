package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.common.Lsn;
import io.modak.common.TableId;
import io.modak.lake.iceberg.IcebergLakeStoragePlugin;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end for MIRRORED tables. Covers registration (publication, slot with
 * exported snapshot, initial copy), the streaming mirror pump folding plain
 * DML into Iceberg via logical replication, and crash/resume, where a stopped
 * pump picks the stream back up from the catalog frontier with no gaps and no duplicates.
 */
class MirrorEndToEndTest {

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static WorkerConfig config;
    private static TableId table;
    private static RegisteredTable meta;

    @BeforeAll
    static void setUpWorld() throws Exception {
        postgres = EmbeddedPostgres.builder()
                .setServerConfig("wal_level", "logical")
                .start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);

        exec("""
                CREATE TABLE public.vehicles (
                    id bigint PRIMARY KEY, vin text NOT NULL, status text, last_seen bigint NOT NULL
                )
                """);
        exec("INSERT INTO public.vehicles VALUES "
                + "(1, 'VIN-001', 'active', 100), (2, 'VIN-002', 'idle', 150), "
                + "(3, 'VIN-003', 'active', 200)");

        config = WorkerConfig.builder()
                .pgUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .warehouse(warehouse.toString())
                .mirrorFlushMillis(200).campaignIntervalSeconds(1).build();

        TableRegistrar.run(config, new String[] {
            "--table", "public.vehicles", "--pk", "id", "--tier-key", "last_seen",
            "--mode", "mirrored",
        });

        catalog = new JdbcCatalog(config.dataSource());
        table = new TableId(Long.parseLong(
                queryOne("SELECT 'public.vehicles'::regclass::oid::bigint::text")));
        meta = catalog.get(table).orElseThrow();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void plainDmlIsMirroredIntoIcebergAndSurvivesAPumpRestart() throws Exception {
        assertEquals(TableMode.MIRRORED, meta.mode());
        assertTrue(catalog.readMirrorFrontier(table).isPresent(), "frontier seeded");
        assertEquals(
                List.of("1|VIN-001|active|100", "2|VIN-002|idle|150", "3|VIN-003|active|200"),
                lakeRows(),
                "initial copy landed under the exported snapshot");

        MirrorWorker worker = newWorker();
        Thread pump = start(worker, "mirror-pump-1");
        try {
            exec("INSERT INTO public.vehicles VALUES (4, 'VIN-004', 'active', 260)");
            exec("UPDATE public.vehicles SET status = 'repair', last_seen = 210 WHERE id = 2");
            exec("DELETE FROM public.vehicles WHERE id = 3");

            awaitFrontierPast(currentWalLsn());
            assertEquals(
                    List.of("1|VIN-001|active|100", "2|VIN-002|repair|210",
                            "4|VIN-004|active|260"),
                    lakeRows(),
                    "insert, update, and delete all trailed into the lake");
        } finally {
            stop(worker, pump);
        }

        // "Crash": the pump is gone, writes keep accumulating in the slot.
        exec("INSERT INTO public.vehicles VALUES (5, 'VIN-005', 'idle', 300)");
        exec("UPDATE public.vehicles SET status = 'idle', last_seen = 310 WHERE id = 4");

        MirrorWorker reborn = newWorker();
        Thread pump2 = start(reborn, "mirror-pump-2");
        try {
            awaitFrontierPast(currentWalLsn());
            assertEquals(
                    List.of("1|VIN-001|active|100", "2|VIN-002|repair|210",
                            "4|VIN-004|idle|310", "5|VIN-005|idle|300"),
                    lakeRows(),
                    "resume from the frontier: no gap, no duplicates");
        } finally {
            stop(reborn, pump2);
        }
    }

    private static MirrorWorker newWorker() {
        return new MirrorWorker(catalog, new IcebergLakeStoragePlugin().create(Map.of()), meta,
                MirrorWorker.Settings.fromConfig(config));
    }

    private static Thread start(MirrorWorker worker, String name) {
        Thread t = new Thread(worker, name);
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void stop(MirrorWorker worker, Thread pump) throws InterruptedException {
        worker.stop();
        pump.join(15_000);
        assertTrue(!pump.isAlive(), "pump thread stopped");
    }

    // With synchronous_commit=off, pg_current_wal_lsn() may lag this session's commits.
    private static long currentWalLsn() {
        return Long.parseLong(
                queryOne("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint::text"));
    }

    private static void awaitFrontierPast(long targetLsn) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            Optional<Lsn> frontier = catalog.readMirrorFrontier(table);
            if (frontier.isPresent() && frontier.get().value() >= targetLsn) {
                return;
            }
            Thread.sleep(100);
        }
        fail("mirror frontier never reached " + targetLsn + " (at "
                + catalog.readMirrorFrontier(table) + ")");
    }

    private static List<String> lakeRows() throws IOException {
        Table lakeTable = new HadoopTables(new Configuration())
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.vehicles");
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(lakeTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("vin") + "|"
                        + r.getField("status") + "|" + r.getField("last_seen"));
            }
        }
        rows.sort(String::compareTo);
        return rows;
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
