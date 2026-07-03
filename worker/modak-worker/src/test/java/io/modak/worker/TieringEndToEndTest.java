package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.TableRegistration;
import io.modak.common.Cutline;
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
 * End-to-end: a real range-partitioned Postgres table ages down into a real Iceberg
 * table on disk, through the full seal → flush → commit → advance → reclaim protocol.
 */
class TieringEndToEndTest {

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
                  (1, 10, 'a'), (2, 20, 'b'),      -- p0 (cold-bound)
                  (3, 110, 'c'),                   -- p1 (cold-bound)
                  (4, 210, 'hot'), (5, 220, 'hot') -- p2 (stays hot)
                """);

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.optional(3, "val", Types.StringType.get()));
        String location = warehouse.resolve("events_cold").toString();
        icebergTable = new HadoopTables(new Configuration())
                .create(schema, PartitionSpec.unpartitioned(), location);

        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                relOid("public.events"), "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER, location, null));
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
    void hotPartitionsAgeDownIntoIcebergAndAreReclaimed() throws Exception {
        TieringWorker worker = new TieringWorker(
                catalog,
                new IcebergLakeStoragePlugin().create(Map.of()),
                new JdbcHotSource(dataSource),
                (t, now) -> List.of(p0, p1),
                new SealGatedEvictionPolicy());

        exec("INSERT INTO modak.read_pins (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) "
                + "VALUES (" + table.oid() + ", 0, 0, now() + interval '1 hour')");

        worker.runCycle(table, NOW);

        icebergTable.refresh();
        assertEquals(List.of("1|10|a", "2|20|b", "3|110|c"), lakeRows());
        long sequenceNumber = icebergTable.currentSnapshot().sequenceNumber();
        assertEquals("200", icebergTable.currentSnapshot().summary().get("modak.new-tier-key-hi"));

        assertEquals(new Cutline(new TierKey(200), new LakeSnapshotId(sequenceNumber)),
                catalog.readCutline(table));
        String props = catalog.get(table).orElseThrow().lakeProps();
        assertTrue(props.contains(".metadata.json"), "metadata_location published: " + props);
        assertTrue(props.contains("snapshot_id"), "snapshot_id published: " + props);

        assertEquals(PartitionState.TIERED, stateOf(p0));
        assertNotNull(queryOne("SELECT to_regclass('public.events_p0')::text"));

        exec("DELETE FROM modak.read_pins");
        TieringWorker sweep = new TieringWorker(catalog,
                new IcebergLakeStoragePlugin().create(Map.of()), new JdbcHotSource(dataSource),
                (t, now) -> List.of(), new SealGatedEvictionPolicy());
        sweep.runCycle(table, NOW);

        assertNull(queryOne("SELECT to_regclass('public.events_p0')::text"), "p0 dropped (DDL)");
        assertNull(queryOne("SELECT to_regclass('public.events_p1')::text"), "p1 dropped (DDL)");
        assertEquals(PartitionState.DROPPED, stateOf(p0));

        assertEquals("2", queryOne("SELECT count(*)::text FROM public.events"));
        assertEquals("2", queryOne("SELECT count(*)::text FROM public.events WHERE event_time >= 200"));
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
