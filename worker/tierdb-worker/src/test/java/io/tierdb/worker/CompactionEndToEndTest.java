package io.tierdb.worker;

import io.tierdb.common.OpKind;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
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
 * End-to-end for the correction path. Rows age into Iceberg (tiering),
 * corrections land in {@code tierdb.delta} (as the Rust router would write
 * them), and one compaction cycle folds them into the base.
 */
class CompactionEndToEndTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static Table icebergTable;
    private static TableId table;

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
        exec("CREATE TABLE public.events_p0 PARTITION OF public.events FOR VALUES FROM (0) TO (200)");
        exec("CREATE TABLE public.events_p2 PARTITION OF public.events FOR VALUES FROM (200) TO (300)");
        exec("INSERT INTO public.events VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 110, 'c'), (4, 210, 'hot')");

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
                "{\"unit\":\"range\"}", IcebergLakeStoragePlugin.IDENTIFIER, location));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        PartitionId p0 = new PartitionId(table, "events_p0");
        catalog.upsertPartition(p0, new PartitionBounds(new TierKey(0), new TierKey(200)),
                PartitionState.HOT);
        catalog.upsertPartition(new PartitionId(table, "events_p2"),
                new PartitionBounds(new TierKey(200), new TierKey(300)), PartitionState.HOT);

        new TieringWorker(catalog, lake(), new JdbcHotSource(dataSource),
                (t, now) -> List.of(p0), new SealGatedEvictionPolicy()).runCycle(table, NOW);
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void correctionsFoldIntoTheBaseAndTheDeltaClears() throws Exception {
        icebergTable.refresh();
        assertEquals(List.of("1|10|a", "2|20|b", "3|110|c"), lakeRows());
        Cutline before = catalog.readCutline(table);
        assertEquals(new TierKey(200), before.t());

        exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES "
                + "(" + table.oid() + ", '2', 0, 20, nextval('tierdb.delta_version'), "
                + "'{\"id\":2,\"event_time\":20,\"val\":\"corrected\"}'), "
                + "(" + table.oid() + ", '3', 1, 110, nextval('tierdb.delta_version'), NULL)");

        CompactionWorker worker = new CompactionWorker(catalog, lake(),
                new JdbcCompactionPolicy(dataSource, catalog, 1000));

        exec("INSERT INTO tierdb.read_pins (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) "
                + "VALUES (" + table.oid() + ", " + before.snapshot().id() + ", 200, now() + interval '1 hour')");
        worker.runCycle(table, NOW);
        assertEquals(before, catalog.readCutline(table), "pinned reader blocks compaction");
        assertEquals("2", queryOne("SELECT count(*)::text FROM tierdb.delta"));

        exec("DELETE FROM tierdb.read_pins");
        worker.runCycle(table, NOW);

        icebergTable.refresh();
        assertEquals(List.of("1|10|a", "2|20|corrected"), lakeRows());

        Cutline after = catalog.readCutline(table);
        assertEquals(new TierKey(200), after.t(), "compaction never moves T");
        assertEquals(icebergTable.currentSnapshot().sequenceNumber(), after.snapshot().id());
        assertTrue(after.snapshot().compareTo(before.snapshot()) > 0, "S advanced");

        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta"), "folded rows cleared");
        String props = catalog.readLakeProps(table).orElseThrow();
        assertTrue(props.contains(".metadata.json"), "metadata_location republished: " + props);

        assertTrue(catalog.findIncompleteOps(table, OpKind.COMPACTION).isEmpty());
        assertEquals("advanced", queryOne(
                "SELECT phase FROM tierdb.op_log WHERE op_kind = 'compaction' ORDER BY updated_at DESC LIMIT 1"));
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
