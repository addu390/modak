package io.modak.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.TableMode;
import io.modak.catalog.TableRegistration;
import io.modak.common.LakeSnapshotId;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.connector.SeamOptions;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;

/**
 * The worker tiers a real partitioned table into Iceberg, delta rows correct
 * cold data, and Spark reads one consistent view through {@link ModakSpark},
 * then writes back through the routed insert path.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ModakSparkSeamTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static TableId table;
    private static String location;
    private static SparkSession spark;
    private static SeamOptions options;

    @BeforeAll
    static void setUpWorld() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);

        exec("""
                CREATE TABLE public.events (
                    id bigint NOT NULL, event_time bigint NOT NULL, val text
                ) PARTITION BY RANGE (event_time)
                """);
        exec("CREATE TABLE public.events_p0 PARTITION OF public.events FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.events_p1 PARTITION OF public.events FOR VALUES FROM (100) TO (200)");
        exec("CREATE TABLE public.events_p2 PARTITION OF public.events FOR VALUES FROM (200) TO (300)");
        exec("""
                INSERT INTO public.events VALUES
                  (1, 10, 'a'), (2, 20, 'b'),      -- p0 (cold-bound)
                  (3, 110, 'c'),                   -- p1 (cold-bound)
                  (4, 210, 'hot'), (5, 220, 'hot') -- p2 (stays hot)
                """);

        org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
                org.apache.iceberg.types.Types.NestedField.required(1, "id",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.required(2, "event_time",
                        org.apache.iceberg.types.Types.LongType.get()),
                org.apache.iceberg.types.Types.NestedField.optional(3, "val",
                        org.apache.iceberg.types.Types.StringType.get()));
        location = warehouse.resolve("events_cold").toString();
        new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
                .create(schema, org.apache.iceberg.PartitionSpec.unpartitioned(), location);

        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                relOid("public.events"), "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER, location, null));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        PartitionId p0 = registerPartition("events_p0", 0, 100);
        PartitionId p1 = registerPartition("events_p1", 100, 200);
        registerPartition("events_p2", 200, 300);

        new TieringWorker(catalog, new IcebergLakeStoragePlugin().create(Map.of()),
                new JdbcHotSource(dataSource), (t, now) -> List.of(p0, p1),
                new SealGatedEvictionPolicy()).runCycle(table, NOW);
        new TieringWorker(catalog, new IcebergLakeStoragePlugin().create(Map.of()),
                new JdbcHotSource(dataSource), (t, now) -> List.of(),
                new SealGatedEvictionPolicy()).runCycle(table, NOW);

        exec("INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload) VALUES ("
                + table.oid() + ", '2', 0, 20, nextval('modak.delta_version'),"
                + " '{\"id\": 2, \"event_time\": 20, \"val\": \"b2\"}')");
        exec("INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload) VALUES ("
                + table.oid() + ", '3', 1, 110, nextval('modak.delta_version'), NULL)");

        spark = SparkSession.builder()
                .master("local[2]")
                .appName("modak-spark-seam-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.catalog.default_iceberg",
                        "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.default_iceberg.type", "hadoop")
                .config("spark.sql.catalog.default_iceberg.warehouse", warehouse.toString())
                .getOrCreate();

        options = SeamOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.events")
                .build();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (spark != null) {
            spark.stop();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    @Order(1)
    void pinnedReadMergesHotColdAndDelta() {
        try (SeamRead read = ModakSpark.read(spark, options)) {
            assertEquals("1", queryOne("SELECT count(*)::text FROM modak.read_pins"));
            assertEquals(List.of("1|10|a", "2|20|b2", "4|210|hot", "5|220|hot"),
                    rows(read.dataframe()));
        }
        assertEquals("0", queryOne("SELECT count(*)::text FROM modak.read_pins"));
    }

    @Test
    @Order(2)
    void pinnedReadIgnoresSnapshotsCommittedAfterTheCapture() {
        try (SeamRead read = ModakSpark.read(spark, options)) {
            spark.createDataFrame(
                            List.of(RowFactory.create(99L, 50L, "late")), eventsSchema())
                    .write().format("iceberg").mode("append").save(location);

            assertEquals(4, read.dataframe().count());
        }
    }

    @Test
    @Order(3)
    void writesRouteAcrossTheSeam() {
        Dataset<Row> batch = spark.createDataFrame(List.of(
                RowFactory.create(6L, 250L, "w-hot"),
                RowFactory.create(7L, 30L, "w-cold")), eventsSchema());

        ModakSpark.write(batch, options);

        assertEquals("1", queryOne("SELECT count(*)::text FROM public.events WHERE id = 6"));
        assertEquals("0|w-cold", queryOne("SELECT op || '|' || (payload ->> 'val')"
                + " FROM modak.delta WHERE table_id = " + table.oid() + " AND pk = '7'"));

        try (SeamRead read = ModakSpark.read(spark, options)) {
            assertEquals(List.of("1|10|a", "2|20|b2", "4|210|hot", "5|220|hot",
                    "6|250|w-hot", "7|30|w-cold"), rows(read.dataframe()));
        }
    }

    @Test
    @Order(4)
    void mirroredTablesReadAndWriteAsAPlainHeap() {
        exec("CREATE TABLE public.readings (id bigint NOT NULL, event_time bigint NOT NULL, val text)");
        exec("INSERT INTO public.readings VALUES (1, 5, 'm1'), (2, 1500, 'm2')");
        TableId mirrored = catalog.register(new TableRegistration(
                relOid("public.readings"), "public", "readings", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER,
                warehouse.resolve("readings_mirror").toString(), null,
                TableMode.MIRRORED, "pub_readings", "slot_readings", Optional.empty()));
        catalog.initCutline(mirrored, new TierKey(1000), new LakeSnapshotId(0));

        SeamOptions mirroredOptions = SeamOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.readings")
                .build();

        try (SeamRead read = ModakSpark.read(spark, mirroredOptions)) {
            assertEquals(List.of("1|5|m1", "2|1500|m2"), rows(read.dataframe()));
        }

        ModakSpark.write(spark.createDataFrame(
                List.of(RowFactory.create(3L, 7L, "m3")), eventsSchema()), mirroredOptions);

        assertEquals("1", queryOne("SELECT count(*)::text FROM public.readings WHERE id = 3"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM modak.delta WHERE table_id = "
                + mirrored.oid()));
        try (SeamRead read = ModakSpark.read(spark, mirroredOptions)) {
            assertEquals(List.of("1|5|m1", "2|1500|m2", "3|7|m3"), rows(read.dataframe()));
        }
    }

    private static StructType eventsSchema() {
        return new StructType()
                .add("id", DataTypes.LongType, false)
                .add("event_time", DataTypes.LongType, false)
                .add("val", DataTypes.StringType, true);
    }

    private static List<String> rows(Dataset<Row> df) {
        return df.collectAsList().stream()
                .map(r -> r.getLong(r.fieldIndex("id")) + "|"
                        + r.getLong(r.fieldIndex("event_time")) + "|"
                        + r.getString(r.fieldIndex("val")))
                .sorted()
                .collect(Collectors.toList());
    }

    private static PartitionId registerPartition(String name, long lo, long hi) {
        PartitionId id = new PartitionId(table, name);
        catalog.upsertPartition(id, new PartitionBounds(new TierKey(lo), new TierKey(hi)),
                PartitionState.HOT);
        return id;
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
