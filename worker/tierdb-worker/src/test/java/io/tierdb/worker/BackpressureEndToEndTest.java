package io.tierdb.worker;

import io.tierdb.worker.cli.TableRegistrar;
import io.tierdb.worker.ops.MirrorWorker;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.Lsn;
import io.tierdb.common.TableId;
import io.tierdb.lake.iceberg.IcebergLakeStoragePlugin;
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
import org.apache.iceberg.Snapshot;
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
 * The mirror pump memory bound: a transaction far larger than maxBufferedRows
 * lands through intermediate folds and is still applied exactly once, published
 * at the transaction boundary.
 */
class BackpressureEndToEndTest {

    private static final int MAX_BUFFERED = 10;
    private static final int TXN_ROWS = 100;

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
                CREATE TABLE public.readings (
                    id bigint PRIMARY KEY, val text NOT NULL, at bigint NOT NULL
                )
                """);
        exec("INSERT INTO public.readings VALUES (0, 'seed', 1)");

        config = WorkerConfig.builder()
                .pgUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .warehouse(warehouse.toString())
                .mirrorFlushMillis(200).campaignIntervalSeconds(1).build();

        TableRegistrar.run(config, new String[] {
            "--table", "public.readings", "--pk", "id", "--tier-key", "at",
            "--mode", "mirrored",
        });

        catalog = new JdbcCatalog(config.dataSource());
        table = new TableId(Long.parseLong(
                queryOne("SELECT 'public.readings'::regclass::oid::bigint::text")));
        meta = catalog.get(table).orElseThrow();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void giantTransactionFoldsIncrementallyAndAppliesExactlyOnce() throws Exception {
        long snapshotsBefore = snapshotCount();

        MirrorWorker worker = new MirrorWorker(catalog,
                new IcebergLakeStoragePlugin().create(Map.of()), meta,
                MirrorWorker.Settings.fromConfig(config).withMaxBufferedRows(MAX_BUFFERED));
        Thread pump = new Thread(worker, "backpressure-pump");
        pump.setDaemon(true);
        pump.start();
        try {
            StringBuilder sql = new StringBuilder("BEGIN;");
            sql.append("INSERT INTO public.readings SELECT g, 'v' || g, g * 10 "
                    + "FROM generate_series(1, ").append(TXN_ROWS).append(") g;");
            sql.append("UPDATE public.readings SET val = 'patched' WHERE id = 1;");
            sql.append("DELETE FROM public.readings WHERE id = 2;");
            sql.append("COMMIT;");
            exec(sql.toString());

            awaitFrontierPast(currentWalLsn());

            List<String> rows = lakeRows();
            assertEquals(TXN_ROWS, rows.size(), "seed + 100 inserts - 1 delete: " + rows);
            assertTrue(rows.contains("0|seed|1"), "pre-existing row intact");
            assertTrue(rows.contains("1|patched|10"), "in-txn update wins over the insert");
            assertTrue(rows.stream().noneMatch(r -> r.startsWith("2|")), "in-txn delete applied");
            assertTrue(rows.contains("100|v100|1000"), "last inserted row landed");

            assertTrue(snapshotCount() > snapshotsBefore + 1,
                    "the giant transaction produced intermediate fold commits");
        } finally {
            worker.stop();
            pump.join(15_000);
            assertTrue(!pump.isAlive(), "pump thread stopped");
        }
    }

    private static long snapshotCount() {
        Table lakeTable = new HadoopTables(new Configuration())
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.readings");
        long n = 0;
        for (Snapshot ignored : lakeTable.snapshots()) {
            n++;
        }
        return n;
    }

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
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.readings");
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(lakeTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("val") + "|" + r.getField("at"));
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
