package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.cdc.ReplicationSource;
import io.modak.common.Lsn;
import io.modak.common.TableId;
import io.modak.lake.LakeStorage;
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
import java.util.function.BooleanSupplier;
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
 * Active/passive worker HA. Two daemons campaign for the leader lease, exactly
 * one leads, and when the leader dies the standby takes over, including the
 * mirrored table's replication slot, within the campaign interval.
 */
class LeaderFailoverTest {

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
                CREATE TABLE public.assets (
                    id bigint PRIMARY KEY, name text NOT NULL, updated_at bigint NOT NULL
                )
                """);
        exec("INSERT INTO public.assets VALUES (1, 'crane', 100)");

        config = new WorkerConfig(
                postgres.getJdbcUrl("postgres", "postgres"), "postgres", "",
                warehouse.toString(), Map.of(),
                1, 0, 0, 1000, 500, 200, 1);

        TableRegistrar.run(config, new String[] {
            "--table", "public.assets", "--pk", "id", "--tier-key", "updated_at",
            "--mode", "mirrored",
        });
        catalog = new JdbcCatalog(config.dataSource());
        table = new TableId(Long.parseLong(
                queryOne("SELECT 'public.assets'::regclass::oid::bigint::text")));
        meta = catalog.get(table).orElseThrow();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void standbyTakesOverWhenTheLeaderDies() throws Exception {
        WorkerDaemon first = new WorkerDaemon(config);
        WorkerDaemon second = new WorkerDaemon(config);
        try {
            first.start();
            await("first daemon leads", first::isLeading);

            second.start();
            Thread.sleep(3_000);
            assertTrue(first.isLeading(), "leader keeps the lease");
            assertFalse(second.isLeading(), "standby waits");

            exec("INSERT INTO public.assets VALUES (2, 'lift', 200)");
            awaitFrontierPast(currentWalLsn());
            assertTrue(lakeRows().contains("2|lift|200"), "leader's pump mirrors");

            first.stop();
            await("standby takes over", second::isLeading);

            exec("INSERT INTO public.assets VALUES (3, 'dozer', 300)");
            awaitFrontierPast(currentWalLsn());
            assertTrue(lakeRows().contains("3|dozer|300"),
                    "the new leader's pump resumed the slot with no manual steps");
        } finally {
            first.stop();
            second.stop();
        }
    }

    @Test
    void slotTakeoverEvictsAZombieHolder() throws Exception {
        // A dead leader's walsender still holds the slot: the new pump must evict it.
        ReplicationSource zombie = ReplicationSource.open(
                config.pgUrl(), config.pgUser(), config.pgPassword(),
                meta.slotName(), meta.publicationName(), Lsn.ZERO);
        MirrorWorker worker = new MirrorWorker(catalog, lake(), meta,
                config.pgUrl(), config.pgUser(), config.pgPassword(),
                config.mirrorBatchRows(), config.mirrorFlushMillis());
        Thread pump = new Thread(worker, "takeover-pump");
        pump.setDaemon(true);
        pump.start();
        try {
            exec("INSERT INTO public.assets VALUES (4, 'mixer', 400)");
            awaitFrontierPast(currentWalLsn());
            assertTrue(lakeRows().contains("4|mixer|400"), "pump streamed past the zombie");
        } finally {
            worker.stop();
            pump.join(15_000);
            try {
                zombie.close();
            } catch (RuntimeException ignored) {
                // Its backend was terminated by the takeover, which is the point.
            }
        }
    }

    private static LakeStorage lake() {
        return new IcebergLakeStoragePlugin().create(Map.of());
    }

    private static void await(String what, BooleanSupplier cond) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("timed out waiting for: " + what);
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
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.assets");
        lakeTable.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(lakeTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("name") + "|"
                        + r.getField("updated_at"));
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
