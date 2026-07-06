package io.tierdb.worker;

import io.tierdb.worker.cli.IngestOperation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.TableMode;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PgValues;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.iceberg.IcebergLakeStoragePlugin;
import io.tierdb.tiering.JdbcHotSource;
import io.tierdb.tiering.policy.SealGatedEvictionPolicy;
import io.tierdb.tiering.TieringWorker;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Files;
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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end bulk ingest: staged Parquet and raw records land in the lake as
 * one upsert commit with no delta rows, and anything outside the cold window
 * is rejected whole.
 */
class BulkIngestEndToEndTest {

    private static final Instant NOW = Instant.parse("2026-07-01T12:00:00Z");

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "event_time", Types.LongType.get()),
            Types.NestedField.optional(3, "val", Types.StringType.get()));

    @TempDir
    static Path warehouse;

    @TempDir
    static Path staging;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static Table icebergTable;
    private static TableId table;
    private static LakeStorage lake;

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
        exec("INSERT INTO public.events VALUES (1, 10, 'a'), (2, 110, 'b'), (3, 210, 'hot')");

        String location = warehouse.resolve("events_cold").toString();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
                .truncate("event_time", 100).build();
        icebergTable = new HadoopTables(new Configuration()).create(SCHEMA, spec, location);

        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                relOid("public.events"), "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"range\",\"partition_width\":100}",
                IcebergLakeStoragePlugin.IDENTIFIER, location,
                TableMode.TIERED, null, null, Optional.empty(), Optional.empty()));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));

        PartitionId p0 = registerPartition("events_p0", 0, 100);
        PartitionId p1 = registerPartition("events_p1", 100, 200);
        registerPartition("events_p2", 200, 300);

        lake = new IcebergLakeStoragePlugin().create(Map.of());
        new TieringWorker(catalog, lake, new JdbcHotSource(dataSource),
                (t, now) -> List.of(p0, p1), new SealGatedEvictionPolicy())
                .runCycle(table, NOW);
        new TieringWorker(catalog, lake, new JdbcHotSource(dataSource),
                (t, now) -> List.of(), new SealGatedEvictionPolicy())
                .runCycle(table, NOW);
        assertEquals(new TierKey(200), catalog.readCutline(table).t());
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void stagedFilesCommitAtomicallyWithoutDeltaRows() throws Exception {
        String fileA = stage("bulk_a.parquet",
                record(10, 30, "bulk-a"), record(11, 40, "bulk-a2"));
        String fileB = stage("bulk_b.parquet", record(12, 130, "bulk-b"));

        long snapshotBefore = catalog.readCutline(table).snapshot().id();
        long advancedBefore = Long.parseLong(queryOne(
                "SELECT count(*)::text FROM tierdb.op_log"
                        + " WHERE op_kind = 'ingest' AND phase = 'advanced'"));
        operation().ingest(catalog.get(table).orElseThrow(),
                List.of(fileA, fileB));

        assertTrue(lakeRows().containsAll(
                List.of("10|30|bulk-a", "11|40|bulk-a2", "12|130|bulk-b")));
        assertTrue(catalog.readCutline(table).snapshot().id() > snapshotBefore,
                "the ingest commit advances S");
        assertEquals(new TierKey(200), catalog.readCutline(table).t(), "T is untouched");
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta"));
        assertEquals(String.valueOf(advancedBefore + 1),
                queryOne("SELECT count(*)::text FROM tierdb.op_log"
                        + " WHERE op_kind = 'ingest' AND phase = 'advanced'"));
    }

    @Test
    void reingestedKeysReplaceTheExistingLakeRows() throws Exception {
        String original = stage("upsert_a.parquet", record(60, 70, "v1"));
        operation().ingest(catalog.get(table).orElseThrow(), List.of(original));
        assertTrue(lakeRows().contains("60|70|v1"));

        String corrected = stage("upsert_b.parquet",
                record(60, 70, "v2"), record(61, 71, "new"));
        operation().ingest(catalog.get(table).orElseThrow(), List.of(corrected));

        List<String> rows = lakeRows();
        assertTrue(rows.contains("60|70|v2"), "existing pk replaced: " + rows);
        assertTrue(rows.contains("61|71|new"), "fresh pk inserted: " + rows);
        assertTrue(!rows.contains("60|70|v1"), "old image gone: " + rows);
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta"));
    }

    @Test
    void recordsAreStagedAndCommittedWithoutTheCallerTouchingParquet() throws Exception {
        Path jsonl = staging.resolve("records.jsonl");
        Files.writeString(jsonl, """
                {"id": 70, "event_time": 80, "val": "from-jsonl"}
                {"id": 71, "event_time": 181, "val": null}
                """);

        RegisteredTable meta = catalog.get(table).orElseThrow();
        List<Column> columns = List.of(
                PgValues.column("id", "bigint", 0, 0),
                PgValues.column("event_time", "bigint", 0, 0),
                PgValues.column("val", "text", 0, 0));
        List<String> staged = operation().stage(meta, columns, jsonl);
        assertEquals(2, staged.size(), "one staged file per partition band");
        operation().ingest(meta, staged);

        List<String> rows = lakeRows();
        assertTrue(rows.contains("70|80|from-jsonl"), rows.toString());
        assertTrue(rows.contains("71|181|null"), rows.toString());
        assertEquals("0", queryOne("SELECT count(*)::text FROM tierdb.delta"));
    }

    @Test
    void recordsOutsideTheIngestWindowAreRejectedBeforeStaging() throws Exception {
        Path jsonl = staging.resolve("hot_record.jsonl");
        Files.writeString(jsonl, "{\"id\": 72, \"event_time\": 250, \"val\": \"hot\"}\n");

        RegisteredTable meta = catalog.get(table).orElseThrow();
        List<Column> columns = List.of(
                PgValues.column("id", "bigint", 0, 0),
                PgValues.column("event_time", "bigint", 0, 0),
                PgValues.column("val", "text", 0, 0));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> operation().stage(meta, columns, jsonl));
        assertTrue(e.getMessage().contains("outside the ingest window"), e.getMessage());
    }

    @Test
    void filesAtOrAboveTheCutLineAreRejectedWhole() throws Exception {
        String cold = stage("mixed_cold.parquet", record(20, 50, "ok"));
        String hot = stage("mixed_hot.parquet", record(21, 250, "belongs-in-the-heap"));

        List<String> before = lakeRows();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> operation().ingest(catalog.get(table).orElseThrow(),
                        List.of(cold, hot)));

        assertTrue(e.getMessage().contains("outside the ingest window"), e.getMessage());
        assertEquals(before, lakeRows(), "nothing committed");
    }

    @Test
    void filesStraddlingAPartitionBoundaryAreRejected() throws Exception {
        String straddling = stage("straddle.parquet",
                record(30, 60, "x"), record(31, 160, "y"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> operation().ingest(catalog.get(table).orElseThrow(),
                        List.of(straddling)));

        assertTrue(e.getMessage().contains("straddles"), e.getMessage());
    }

    @Test
    void filesBelowTheRetentionLineAreRejected() throws Exception {
        exec("UPDATE tierdb.cutline SET retention_line = 100 WHERE table_id = " + table.oid());
        String expired = stage("expired.parquet", record(40, 50, "too-old"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> operation().ingest(catalog.get(table).orElseThrow(),
                        List.of(expired)));

        assertTrue(e.getMessage().contains("outside the ingest window"), e.getMessage());
        exec("UPDATE tierdb.cutline SET retention_line = NULL WHERE table_id = " + table.oid());
    }

    @Test
    void fullyMirroredTablesAreRejected() throws Exception {
        exec("CREATE TABLE public.readings (id bigint NOT NULL, event_time bigint NOT NULL)");
        TableId mirrored = catalog.register(new TableRegistration(
                relOid("public.readings"), "public", "readings", List.of("id"), "event_time",
                "{\"unit\":\"range\",\"partition_width\":100}",
                IcebergLakeStoragePlugin.IDENTIFIER,
                warehouse.resolve("readings_mirror").toString(),
                TableMode.MIRRORED, "pub_r", "slot_r", Optional.empty(), Optional.empty()));
        catalog.initCutline(mirrored, new TierKey(Long.MIN_VALUE), new LakeSnapshotId(0));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> operation().ingest(catalog.get(mirrored).orElseThrow(),
                        List.of("unused.parquet")));
        assertTrue(e.getMessage().contains("source of truth"), e.getMessage());
    }

    @Test
    void mirroredTablesWithHeapRetentionIngestBelowTheDropBoundary() throws Exception {
        exec("CREATE TABLE public.metrics (id bigint NOT NULL, event_time bigint NOT NULL, val text)");
        String location = warehouse.resolve("metrics_mirror").toString();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
                .truncate("event_time", 100).build();
        Table mirrorTable = new HadoopTables(new Configuration())
                .create(SCHEMA, spec, location);

        TableId mirrored = catalog.register(new TableRegistration(
                relOid("public.metrics"), "public", "metrics", List.of("id"), "event_time",
                "{\"unit\":\"range\",\"partition_width\":100}",
                IcebergLakeStoragePlugin.IDENTIFIER, location,
                TableMode.MIRRORED, "pub_m", "slot_m", Optional.of(100L), Optional.empty()));
        catalog.initCutline(mirrored, new TierKey(200), new LakeSnapshotId(0));

        String backfill = stage("metrics_backfill.parquet", record(50, 30, "backfill"));
        operation().ingest(catalog.get(mirrored).orElseThrow(),
                List.of(backfill));

        mirrorTable.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(mirrorTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("event_time")
                        + "|" + r.getField("val"));
            }
        }
        assertEquals(List.of("50|30|backfill"), rows);
        assertTrue(catalog.readCutline(mirrored).snapshot().id() > 0, "S advanced");

        String hot = stage("metrics_hot.parquet", record(51, 250, "still-in-the-heap"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> operation().ingest(catalog.get(mirrored).orElseThrow(),
                        List.of(hot)));
        assertTrue(e.getMessage().contains("outside the ingest window"), e.getMessage());
    }

    private static IngestOperation operation() {
        return new IngestOperation(catalog, lake);
    }

    private static Record record(long id, long eventTime, String val) {
        Record r = GenericRecord.create(SCHEMA);
        r.setField("id", id);
        r.setField("event_time", eventTime);
        r.setField("val", val);
        return r;
    }

    private static String stage(String name, Record... records) throws IOException {
        Path file = staging.resolve(name);
        Files.deleteIfExists(file);
        GenericAppenderFactory factory = new GenericAppenderFactory(SCHEMA);
        try (FileAppender<Record> appender = factory.newAppender(
                org.apache.iceberg.Files.localOutput(file.toFile()), FileFormat.PARQUET)) {
            appender.addAll(List.of(records));
        }
        return file.toString();
    }

    private static PartitionId registerPartition(String name, long lo, long hi) {
        PartitionId id = new PartitionId(table, name);
        catalog.upsertPartition(id, new PartitionBounds(new TierKey(lo), new TierKey(hi)),
                PartitionState.HOT);
        return id;
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
