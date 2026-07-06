package io.tierdb.worker;

import io.tierdb.worker.cli.TableRegistrar;
import io.tierdb.worker.ops.MirrorRetention;
import io.tierdb.worker.ops.MirrorWorker;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.Lsn;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.compaction.CompactionWorker;
import io.tierdb.compaction.JdbcCompactionPolicy;
import io.tierdb.lake.LakeStorage;
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
 * End-to-end for mirrored tables with heap retention: partitions age out of the
 * heap, T rises to the drop boundary, and below-window corrections written to
 * {@code tierdb.delta} are folded into the lake mirror by the pump itself.
 */
class MirroredRetentionFoldTest {

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
                    id bigint NOT NULL, val text, event_time bigint NOT NULL,
                    PRIMARY KEY (id, event_time)
                ) PARTITION BY RANGE (event_time)
                """);
        exec("CREATE TABLE public.readings_p0 PARTITION OF public.readings "
                + "FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.readings_p1 PARTITION OF public.readings "
                + "FOR VALUES FROM (100) TO (200)");
        exec("CREATE TABLE public.readings_p2 PARTITION OF public.readings "
                + "FOR VALUES FROM (200) TO (300)");
        exec("INSERT INTO public.readings VALUES "
                + "(1, 'a', 10), (2, 'b', 20), (3, 'c', 110), (4, 'hot', 210)");

        config = WorkerConfig.builder()
                .pgUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .warehouse(warehouse.toString())
                .mirrorFlushMillis(200).campaignIntervalSeconds(1).build();

        TableRegistrar.run(config, new String[] {
            "--table", "public.readings", "--pk", "id", "--tier-key", "event_time",
            "--mode", "mirrored", "--heap-retention", "100",
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
    void belowWindowCorrectionsFoldIntoTheMirrorAfterAPartitionDrop() throws Exception {
        assertEquals(Optional.of(100L), meta.heapRetentionLag());
        assertEquals(List.of("1|a|10", "2|b|20", "3|c|110", "4|hot|210"), lakeRows());

        MirrorWorker worker = new MirrorWorker(catalog, lake(), meta,
                MirrorWorker.Settings.fromConfig(config).withDeltaFold(
                        new CompactionWorker(catalog, lake(),
                                new JdbcCompactionPolicy(dataSource, catalog, 1000)),
                        200));
        Thread pump = new Thread(worker, "retention-fold-pump");
        pump.setDaemon(true);
        pump.start();
        try {
            exec("INSERT INTO public.readings VALUES (5, 'newest', 250)");
            awaitFrontierPast(currentWalLsn());

            MirrorRetention retention = new MirrorRetention(dataSource, catalog);
            retention.run(meta);
            exec("UPDATE public.readings SET val = 'still-hot' WHERE id = 4");
            awaitFrontierPast(currentWalLsn());
            retention.run(meta);

            assertNull(queryOne("SELECT to_regclass('public.readings_p0')::text"),
                    "aged partition dropped from the heap");
            assertEquals(new TierKey(100), catalog.readCutline(table).t(),
                    "T rose to the drop boundary");
            assertEquals(List.of("1|a|10", "2|b|20", "3|c|110", "4|still-hot|210",
                    "5|newest|250"), lakeRows(), "the lake still holds the dropped rows");

            exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES "
                    + "(" + table.oid() + ", '1', 0, 10, nextval('tierdb.delta_version'), "
                    + "'{\"id\":1,\"val\":\"corrected\",\"event_time\":10}'), "
                    + "(" + table.oid() + ", '2', 1, 20, nextval('tierdb.delta_version'), "
                    + "'{\"id\":2}')");

            awaitEmptyDelta();
            assertEquals(List.of("1|corrected|10", "3|c|110", "4|still-hot|210",
                    "5|newest|250"), lakeRows(),
                    "upsert replaced the mirrored row, tombstone removed one");

            exec("INSERT INTO public.readings VALUES (6, 'post-fold', 260)");
            awaitFrontierPast(currentWalLsn());
            assertEquals(List.of("1|corrected|10", "3|c|110", "4|still-hot|210",
                    "5|newest|250", "6|post-fold|260"), lakeRows());
        } finally {
            worker.stop();
            pump.join(15_000);
            assertTrue(!pump.isAlive(), "pump thread stopped");
        }
    }

    private static LakeStorage lake() {
        return new IcebergLakeStoragePlugin().create(Map.of());
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

    private static void awaitEmptyDelta() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            if ("0".equals(queryOne("SELECT count(*)::text FROM tierdb.delta"))) {
                return;
            }
            Thread.sleep(100);
        }
        fail("delta backlog never folded: " + queryOne(
                "SELECT count(*)::text FROM tierdb.delta"));
    }

    private static List<String> lakeRows() throws IOException {
        Table lakeTable = new HadoopTables(new Configuration())
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.readings");
        lakeTable.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(lakeTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("val") + "|"
                        + r.getField("event_time"));
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
