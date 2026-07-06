package io.tierdb.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.TableMode;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.Cutline;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.compaction.CompactionWorker;
import io.tierdb.compaction.JdbcCompactionPolicy;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.iceberg.IcebergLakeStoragePlugin;
import io.tierdb.tiering.JdbcHotSource;
import io.tierdb.tiering.policy.SealGatedEvictionPolicy;
import io.tierdb.tiering.TieringWorker;
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

/** End-to-end for keep-heap: partitions tier without dropping, and plain DML on tiered rows feeds the delta through the mirror trigger. */
class KeepHeapEndToEndTest {

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
                  (1, 10, 'a'), (2, 20, 'b'),      -- p0 (tiers, kept)
                  (3, 110, 'c'),                   -- p1 (tiers, kept)
                  (4, 210, 'hot')                  -- p2 (stays hot)
                """);
        stubColdMirror();

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
                "{\"unit\":\"range-100\"}", IcebergLakeStoragePlugin.IDENTIFIER, location,
                TableMode.TIERED, null, null, Optional.empty(), Optional.empty(), true));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        p0 = registerPartition("events_p0", 0, 100);
        p1 = registerPartition("events_p1", 100, 200);
        registerPartition("events_p2", 200, 300);
    }

    private static void stubColdMirror() {
        exec("CREATE SCHEMA IF NOT EXISTS tierdb");
        exec("""
                CREATE FUNCTION tierdb.cold_mirror() RETURNS trigger LANGUAGE plpgsql AS $body$
                DECLARE tid bigint := TG_ARGV[0]::bigint;
                BEGIN
                    IF TG_OP = 'DELETE' THEN
                        INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload)
                        VALUES (tid, OLD.id::text, 1, OLD.event_time,
                                nextval('tierdb.delta_version'), jsonb_build_object('id', OLD.id))
                        ON CONFLICT (table_id, pk) DO UPDATE
                          SET op = 1, tier_key = EXCLUDED.tier_key,
                              version = EXCLUDED.version, payload = EXCLUDED.payload,
                              updated_at = now();
                        RETURN NULL;
                    END IF;
                    INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload)
                    VALUES (tid, NEW.id::text, 0, NEW.event_time,
                            nextval('tierdb.delta_version'), to_jsonb(NEW))
                    ON CONFLICT (table_id, pk) DO UPDATE
                      SET op = 0, tier_key = EXCLUDED.tier_key,
                          version = EXCLUDED.version, payload = EXCLUDED.payload,
                          updated_at = now();
                    RETURN NULL;
                END
                $body$
                """);
        exec("""
                CREATE FUNCTION tierdb_attach_cold_mirror(t oid, p oid) RETURNS text
                LANGUAGE plpgsql AS $body$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_trigger
                                   WHERE tgrelid = p AND tgname = 'tierdb_cold_mirror') THEN
                        EXECUTE format('CREATE TRIGGER tierdb_cold_mirror
                                        AFTER INSERT OR UPDATE OR DELETE ON %s
                                        FOR EACH ROW EXECUTE FUNCTION tierdb.cold_mirror(%L)',
                                       p::regclass, t);
                    END IF;
                    RETURN 'attached';
                END
                $body$
                """);
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void keepHeapTiersWithoutDroppingAndMirrorsPlainDmlIntoTheLake() throws Exception {
        new TieringWorker(catalog, lake(), new JdbcHotSource(dataSource),
                (t, now) -> List.of(p0, p1), new SealGatedEvictionPolicy()).runCycle(table, NOW);

        icebergTable.refresh();
        assertEquals(List.of("1|10|a", "2|20|b", "3|110|c"), lakeRows());
        assertEquals(new TierKey(200), catalog.readCutline(table).t());

        assertNotNull(queryOne("SELECT to_regclass('public.events_p0')::text"), "p0 kept");
        assertNotNull(queryOne("SELECT to_regclass('public.events_p1')::text"), "p1 kept");
        assertEquals(PartitionState.TIERED, stateOf(p0));
        assertEquals(PartitionState.TIERED, stateOf(p1));
        assertEquals("4", queryOne("SELECT count(*)::text FROM public.events"),
                "the heap keeps every row");
        assertEquals("2", queryOne("SELECT count(*)::text FROM pg_trigger "
                + "WHERE tgname = 'tierdb_cold_mirror'"), "both tiered partitions mirrored");

        new TieringWorker(catalog, lake(), new JdbcHotSource(dataSource),
                (t, now) -> List.of(), new SealGatedEvictionPolicy()).runCycle(table, NOW);
        assertNotNull(queryOne("SELECT to_regclass('public.events_p0')::text"));
        assertEquals(PartitionState.TIERED, stateOf(p0));

        exec("UPDATE public.events SET val = 'corrected' WHERE id = 2");
        exec("DELETE FROM public.events WHERE id = 3");
        assertEquals("corrected",
                queryOne("SELECT payload ->> 'val' FROM tierdb.delta WHERE pk = '2'"));
        assertEquals("1", queryOne("SELECT op::text FROM tierdb.delta WHERE pk = '3'"));

        Cutline before = catalog.readCutline(table);
        new CompactionWorker(catalog, lake(),
                new JdbcCompactionPolicy(dataSource, catalog, 1000)).runCycle(table, NOW);

        icebergTable.refresh();
        assertEquals(List.of("1|10|a", "2|20|corrected"), lakeRows());
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta"));
        assertEquals(new TierKey(200), catalog.readCutline(table).t(), "T never moves");
        assertTrue(catalog.readCutline(table).snapshot().compareTo(before.snapshot()) > 0);

        assertEquals("corrected", queryOne("SELECT val FROM public.events WHERE id = 2"));
        assertEquals("0", queryOne("SELECT count(*)::text FROM public.events WHERE id = 3"));
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

    private static LakeStorage lake() {
        return new IcebergLakeStoragePlugin().create(Map.of());
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
