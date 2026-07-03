package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * End-to-end lake retention. Tiered partitions age into Iceberg, then the
 * retention pass expires rows below R = T - lake_retention_lag, purges stale
 * delta rows, and records the line, all deferred while a reader holds a pin.
 */
class RetentionEndToEndTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static Table icebergTable;
    private static TableId table;
    private static PartitionId p0;
    private static PartitionId p1;

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
                  (1, 10, 'a'), (2, 20, 'b'),      -- p0: expires
                  (3, 110, 'c'),                   -- p1: tiered, survives retention
                  (4, 210, 'hot')                  -- p2: stays hot
                """);

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.optional(3, "val", Types.StringType.get()));
        String location = warehouse.resolve("events_cold").toString();
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .truncate("event_time", 100).build();
        icebergTable = new HadoopTables(new Configuration()).create(schema, spec, location);

        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                relOid("public.events"), "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"range\",\"partition_width\":100}",
                IcebergLakeStoragePlugin.IDENTIFIER, location, null,
                TableMode.TIERED, null, null, Optional.empty(), Optional.of(50L)));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        p0 = registerPartition("events_p0", 0, 100);
        p1 = registerPartition("events_p1", 100, 200);
        registerPartition("events_p2", 200, 300);
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void retentionExpiresLakeRowsBelowTheBoundary() throws Exception {
        var lake = new IcebergLakeStoragePlugin().create(Map.of());
        new TieringWorker(catalog, lake, new JdbcHotSource(dataSource),
                (t, now) -> List.of(p0, p1), new SealGatedEvictionPolicy())
                .runCycle(table, NOW);
        new TieringWorker(catalog, lake, new JdbcHotSource(dataSource),
                (t, now) -> List.of(), new SealGatedEvictionPolicy())
                .runCycle(table, NOW);
        assertEquals(new TierKey(200), catalog.readCutline(table).t());
        assertEquals(PartitionState.DROPPED, stateOf(p0));
        assertEquals(List.of("1|10|a", "2|20|b", "3|110|c"), lakeRows());

        exec("INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload) VALUES "
                + "(" + table.oid() + ", '1', 0, 10, 1, '{\"id\":1}'), "
                + "(" + table.oid() + ", '3', 0, 110, 2, '{\"id\":3}')");

        RetentionWorker retention = new RetentionWorker(catalog, lake);
        exec("INSERT INTO modak.read_pins (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) "
                + "VALUES (" + table.oid() + ", 0, 0, now() + interval '1 hour')");
        retention.runCycle(catalog.get(table).orElseThrow());
        assertTrue(catalog.readRetentionLine(table).isEmpty(), "pin defers retention");

        exec("DELETE FROM modak.read_pins");
        long snapshotBefore = catalog.readCutline(table).snapshot().id();
        retention.runCycle(catalog.get(table).orElseThrow());

        assertEquals(new TierKey(100), catalog.readRetentionLine(table).orElseThrow());
        assertEquals(List.of("3|110|c"), lakeRows(), "rows below R are gone");
        assertTrue(catalog.readCutline(table).snapshot().id() > snapshotBefore,
                "the delete commit advances S");
        assertEquals(new TierKey(200), catalog.readCutline(table).t(), "T is untouched");
        assertEquals("1", queryOne("SELECT count(*)::text FROM modak.delta"));
        assertEquals("3", queryOne("SELECT pk FROM modak.delta"));
        assertTrue(catalog.listPartitions(table).stream()
                .noneMatch(p -> p.id().equals(p0)), "p0's catalog row below R is gone");
        assertTrue(catalog.get(table).orElseThrow().lakeProps().contains("snapshot_id"));

        long snapshotAfter = catalog.readCutline(table).snapshot().id();
        retention.runCycle(catalog.get(table).orElseThrow());
        assertEquals(snapshotAfter, catalog.readCutline(table).snapshot().id());
        assertEquals(new TierKey(100), catalog.readRetentionLine(table).orElseThrow());
    }

    private static PartitionId registerPartition(String name, long lo, long hi) {
        PartitionId id = new PartitionId(table, name);
        catalog.upsertPartition(id, new PartitionBounds(new TierKey(lo), new TierKey(hi)),
                PartitionState.HOT);
        return id;
    }

    private static PartitionState stateOf(PartitionId id) {
        return catalog.listPartitions(table).stream()
                .filter(p -> p.id().equals(id)).findFirst().orElseThrow().state();
    }

    private static List<String> lakeRows() throws IOException {
        icebergTable.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(icebergTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("event_time") + "|" + r.getField("val"));
            }
        }
        rows.sort(String::compareTo);
        return rows;
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
