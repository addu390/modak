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
import io.modak.compaction.CompactionWorker;
import io.modak.compaction.JdbcCompactionPolicy;
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
 * Composite-PK variants of the two write paths that carry key identity into the
 * lake: the compaction fold (router-shaped delta entries with multi-column keys,
 * tombstone payloads carrying the pk fields) and the CDC mirror pump (canonical
 * joined keys, equality deletes over every pk column).
 */
class CompositePkEndToEndTest {

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
    void compactionFoldsCompositeKeyUpsertsAndTombstones() throws Exception {
        exec("""
                CREATE TABLE public.readings (
                    tenant_id bigint NOT NULL, device_id text NOT NULL,
                    event_time bigint NOT NULL, val text
                ) PARTITION BY RANGE (event_time)
                """);
        exec("CREATE TABLE public.readings_p0 PARTITION OF public.readings"
                + " FOR VALUES FROM (0) TO (200)");
        exec("CREATE TABLE public.readings_p2 PARTITION OF public.readings"
                + " FOR VALUES FROM (200) TO (300)");
        exec("INSERT INTO public.readings VALUES "
                + "(1, 'd1', 10, 'a'), (1, 'd2', 20, 'b'), (2, 'd1', 110, 'c'), "
                + "(1, 'd1', 210, 'hot')");

        Schema schema = new Schema(
                Types.NestedField.required(1, "tenant_id", Types.LongType.get()),
                Types.NestedField.required(2, "device_id", Types.StringType.get()),
                Types.NestedField.required(3, "event_time", Types.LongType.get()),
                Types.NestedField.optional(4, "val", Types.StringType.get()));
        String location = warehouse.resolve("readings_cold").toString();
        Table icebergTable = new HadoopTables(new Configuration())
                .create(schema, PartitionSpec.unpartitioned(), location);

        TableId table = catalog.register(new TableRegistration(
                relOid("public.readings"), "public", "readings",
                List.of("tenant_id", "device_id"), "event_time",
                "{\"unit\":\"range\"}", IcebergLakeStoragePlugin.IDENTIFIER, location, null));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        PartitionId p0 = new PartitionId(table, "readings_p0");
        catalog.upsertPartition(p0, new PartitionBounds(new TierKey(0), new TierKey(200)),
                PartitionState.HOT);
        catalog.upsertPartition(new PartitionId(table, "readings_p2"),
                new PartitionBounds(new TierKey(200), new TierKey(300)), PartitionState.HOT);
        new TieringWorker(catalog, lake(), new JdbcHotSource(dataSource),
                (t, now) -> List.of(p0), new SealGatedEvictionPolicy()).runCycle(table, NOW);

        icebergTable.refresh();
        assertEquals(List.of("1|d1|10|a", "1|d2|20|b", "2|d1|110|c"), lakeRows(icebergTable));

        // What the Rust router writes: joined pk, upsert = full row, tombstone = pk fields.
        exec("INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload) VALUES "
                + "(" + table.oid() + ", '1' || chr(31) || 'd2', 0, 20, "
                + "nextval('modak.delta_version'), "
                + "'{\"tenant_id\":1,\"device_id\":\"d2\",\"event_time\":20,\"val\":\"corrected\"}'), "
                + "(" + table.oid() + ", '2' || chr(31) || 'd1', 1, 110, "
                + "nextval('modak.delta_version'), '{\"tenant_id\":2,\"device_id\":\"d1\"}')");

        new CompactionWorker(catalog, lake(),
                new JdbcCompactionPolicy(dataSource, catalog, 1000)).runCycle(table, NOW);

        icebergTable.refresh();
        assertEquals(List.of("1|d1|10|a", "1|d2|20|corrected"), lakeRows(icebergTable),
                "upsert replaced (1,d2); tombstone removed (2,d1); (1,d1) untouched");
        assertEquals("0", queryOne("SELECT count(*)::text FROM modak.delta"));
    }

    @Test
    void mirrorPumpFoldsDmlByCompositeKey() throws Exception {
        exec("""
                CREATE TABLE public.locations (
                    tenant_id bigint NOT NULL, vehicle_id text NOT NULL,
                    lat double precision, updated_at bigint NOT NULL,
                    PRIMARY KEY (tenant_id, vehicle_id)
                )
                """);
        exec("INSERT INTO public.locations VALUES "
                + "(1, 'V1', 1.0, 100), (1, 'V2', 2.0, 150), (2, 'V1', 3.0, 200)");

        TableRegistrar.run(config, new String[] {
            "--table", "public.locations", "--pk", "tenant_id,vehicle_id",
            "--tier-key", "updated_at", "--mode", "mirrored",
        });
        TableId table = new TableId(Long.parseLong(
                queryOne("SELECT 'public.locations'::regclass::oid::bigint::text")));
        RegisteredTable meta = catalog.get(table).orElseThrow();
        assertEquals(List.of("tenant_id", "vehicle_id"), meta.primaryKeyCols());

        Table lakeTable = new HadoopTables(new Configuration())
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.locations");
        assertEquals(List.of("1|V1|1.0|100", "1|V2|2.0|150", "2|V1|3.0|200"),
                locationRows(lakeTable), "initial copy");

        MirrorWorker worker = new MirrorWorker(catalog, lake(), meta,
                MirrorWorker.Settings.fromConfig(config));
        Thread pump = new Thread(worker, "composite-mirror-pump");
        pump.setDaemon(true);
        pump.start();
        try {
            // Same vehicle_id, two tenants: only tenant 2 dies, only tenant 1 moves.
            exec("UPDATE public.locations SET lat = 9.0, updated_at = 210"
                    + " WHERE tenant_id = 1 AND vehicle_id = 'V1'");
            exec("DELETE FROM public.locations WHERE tenant_id = 2 AND vehicle_id = 'V1'");
            exec("INSERT INTO public.locations VALUES (2, 'V2', 4.0, 260)");

            awaitFrontierPast(table, currentWalLsn());
            assertEquals(
                    List.of("1|V1|9.0|210", "1|V2|2.0|150", "2|V2|4.0|260"),
                    locationRows(lakeTable));
        } finally {
            worker.stop();
            pump.join(15_000);
            assertTrue(!pump.isAlive(), "pump thread stopped");
        }
    }

    private static LakeStorage lake() {
        return new IcebergLakeStoragePlugin().create(Map.of());
    }

    private static List<String> lakeRows(Table icebergTable) throws IOException {
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(icebergTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("tenant_id") + "|" + r.getField("device_id") + "|"
                        + r.getField("event_time") + "|" + r.getField("val"));
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static List<String> locationRows(Table lakeTable) throws IOException {
        lakeTable.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(lakeTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("tenant_id") + "|" + r.getField("vehicle_id") + "|"
                        + r.getField("lat") + "|" + r.getField("updated_at"));
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

    private static long relOid(String qualified) {
        return Long.parseLong(queryOne("SELECT '" + qualified + "'::regclass::oid::bigint::text"));
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
