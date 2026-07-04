package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableRegistration;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.lake.LakeStorage;
import io.modak.lake.iceberg.IcebergLakeStoragePlugin;
import io.modak.tiering.JdbcHotSource;
import io.modak.tiering.SealGatedEvictionPolicy;
import io.modak.tiering.TieringWorker;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Additive schema evolution end to end, on both write paths: a mirrored table's
 * pump flushes in-flight rows then evolves Iceberg on ADD COLUMN (and dies
 * cleanly on destructive DDL), and a tiered table's flush adds heap columns the
 * lake has never seen before writing.
 */
class SchemaEvolutionEndToEndTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
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
        catalog = new JdbcCatalog(config.dataSource());
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void mirroredAddColumnEvolvesTheLakeAndDestructiveDdlKillsThePump() throws Exception {
        exec("""
                CREATE TABLE public.fleet (
                    id bigint PRIMARY KEY, vin text NOT NULL, last_seen bigint NOT NULL
                )
                """);
        exec("INSERT INTO public.fleet VALUES (1, 'VIN-001', 100)");

        TableRegistrar.run(config, new String[] {
            "--table", "public.fleet", "--pk", "id", "--tier-key", "last_seen",
            "--mode", "mirrored",
        });
        TableId table = new TableId(Long.parseLong(
                queryOne("SELECT 'public.fleet'::regclass::oid::bigint::text")));
        RegisteredTable meta = catalog.get(table).orElseThrow();

        MirrorWorker worker = new MirrorWorker(catalog, lake(), meta,
                MirrorWorker.Settings.fromConfig(config));
        Thread pump = new Thread(worker, "evo-mirror-pump");
        pump.setDaemon(true);
        pump.start();
        try {
            exec("INSERT INTO public.fleet VALUES (2, 'VIN-002', 150)");
            exec("ALTER TABLE public.fleet ADD COLUMN color text");
            exec("INSERT INTO public.fleet VALUES (3, 'VIN-003', 200, 'red')");
            exec("UPDATE public.fleet SET color = 'blue', last_seen = 210 WHERE id = 1");

            awaitFrontierPast(table, currentWalLsn());
            assertEquals(
                    List.of("1|VIN-001|210|blue", "2|VIN-002|150|null", "3|VIN-003|200|red"),
                    fleetRows(),
                    "pre-DDL rows read NULL for the new column; post-DDL rows carry it");

            // Destructive: the pump must die, not diverge or spin.
            exec("ALTER TABLE public.fleet DROP COLUMN vin");
            exec("INSERT INTO public.fleet VALUES (4, 260, 'green')");
            pump.join(30_000);
            assertTrue(!pump.isAlive(), "destructive DDL kills the table's pump thread");
        } finally {
            worker.stop();
            pump.join(15_000);
        }
    }

    @Test
    void tieredFlushAddsNewHeapColumnsBeforeWriting() throws Exception {
        exec("""
                CREATE TABLE public.metrics (
                    id bigint NOT NULL, event_time bigint NOT NULL, val text
                ) PARTITION BY RANGE (event_time)
                """);
        exec("CREATE TABLE public.metrics_p0 PARTITION OF public.metrics"
                + " FOR VALUES FROM (0) TO (200)");
        exec("CREATE TABLE public.metrics_p2 PARTITION OF public.metrics"
                + " FOR VALUES FROM (200) TO (300)");
        exec("INSERT INTO public.metrics VALUES (1, 10, 'a'), (2, 110, 'b')");

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.optional(3, "val", Types.StringType.get()));
        String location = warehouse.resolve("metrics_cold").toString();
        Table icebergTable = new HadoopTables(new Configuration())
                .create(schema, PartitionSpec.unpartitioned(), location);

        TableId table = catalog.register(new TableRegistration(
                Long.parseLong(queryOne("SELECT 'public.metrics'::regclass::oid::bigint::text")),
                "public", "metrics", List.of("id"), "event_time",
                "{\"unit\":\"range\"}", IcebergLakeStoragePlugin.IDENTIFIER, location, null));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        PartitionId p0 = new PartitionId(table, "metrics_p0");
        PartitionId p2 = new PartitionId(table, "metrics_p2");
        catalog.upsertPartition(p0, new PartitionBounds(new TierKey(0), new TierKey(200)),
                PartitionState.HOT);
        catalog.upsertPartition(p2, new PartitionBounds(new TierKey(200), new TierKey(300)),
                PartitionState.HOT);

        new TieringWorker(catalog, lake(), new JdbcHotSource(dataSource),
                (t, now) -> List.of(p0), new SealGatedEvictionPolicy()).runCycle(table, NOW);
        icebergTable.refresh();
        assertEquals(List.of("1|10|a|-", "2|110|b|-"), metricRows(icebergTable, false),
                "first flush under the original schema");

        exec("ALTER TABLE public.metrics ADD COLUMN unit text");
        exec("INSERT INTO public.metrics VALUES (3, 210, 'c', 'ms')");

        new TieringWorker(catalog, lake(), new JdbcHotSource(dataSource),
                (t, now) -> List.of(p2), new SealGatedEvictionPolicy()).runCycle(table, NOW);
        icebergTable.refresh();
        assertEquals(List.of("1|10|a|null", "2|110|b|null", "3|210|c|ms"),
                metricRows(icebergTable, true),
                "flush evolved the schema: old rows NULL, new row typed");
    }

    private static LakeStorage lake() {
        return new IcebergLakeStoragePlugin().create(Map.of());
    }

    private static List<String> fleetRows() throws IOException {
        Table lakeTable = new HadoopTables(new Configuration())
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.fleet");
        lakeTable.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(lakeTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("vin") + "|"
                        + r.getField("last_seen") + "|" + r.getField("color"));
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static List<String> metricRows(Table icebergTable, boolean withUnit)
            throws IOException {
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(icebergTable).build()) {
            for (Record r : records) {
                String unit = withUnit ? String.valueOf(r.getField("unit")) : "-";
                rows.add(r.getField("id") + "|" + r.getField("event_time") + "|"
                        + r.getField("val") + "|" + unit);
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static long currentWalLsn() {
        return Long.parseLong(
                queryOne("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint::text"));
    }

    private static void awaitFrontierPast(TableId table, long targetLsn)
            throws InterruptedException {
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
