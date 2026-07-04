package io.modak.worker;

import io.modak.common.OpPhase;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.LoadLabel;
import io.modak.catalog.LoadState;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.catalog.TableRegistration;
import io.modak.common.LakeSnapshotId;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.modak.lake.LakeStorage;
import io.modak.lake.iceberg.IcebergLakeStoragePlugin;
import io.modak.load.LoadClient;
import io.modak.load.LoadOptions;
import io.modak.load.LoadRequest;
import io.modak.load.LoadResult;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Staged loads through {@link LoadClient}, adopted by {@link LoadAdoptionWorker}
 * as one lake commit per cycle, including crash-resume and the retention guard.
 */
class LoadAdoptionEndToEndTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "event_time", Types.LongType.get()),
            Types.NestedField.optional(3, "val", Types.StringType.get()));

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static LakeStorage lake;

    private Table icebergTable;
    private TableId table;

    private static int tableSeq;

    @BeforeAll
    static void setUpWorld() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);
        catalog = new JdbcCatalog(dataSource);
        lake = new IcebergLakeStoragePlugin().create(Map.of());
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @BeforeEach
    void freshTable() {
        // A fresh heap + lake table per test keeps snapshot counting exact.
        tableSeq++;
        String name = "events_" + tableSeq;
        exec("CREATE TABLE public." + name
                + " (id bigint PRIMARY KEY, event_time bigint NOT NULL, val text)");

        String location = warehouse.resolve(name + "_cold").toString();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA)
                .truncate("event_time", 100).build();
        icebergTable = new HadoopTables(new Configuration()).create(SCHEMA, spec, location);

        table = catalog.register(new TableRegistration(
                1000L + tableSeq, "public", name, List.of("id"), "event_time",
                "{\"unit\":\"range\",\"partition_width\":100}",
                IcebergLakeStoragePlugin.IDENTIFIER, location, null,
                TableMode.TIERED, null, null, Optional.empty(), Optional.empty()));
        catalog.initCutline(table, new TierKey(200), new LakeSnapshotId(0));
    }

    private LoadClient client(int spoolThreshold) {
        return new LoadClient(LoadOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.events_" + tableSeq)
                .spoolThreshold(spoolThreshold)
                .build(), format -> lake);
    }

    private static Map<String, Object> row(long id, long ts, String v) {
        return Map.of("id", id, "event_time", ts, "val", v);
    }

    @Test
    void stagedLabelsBecomeOneCommitAndTheirRowsBecomeVisible() throws Exception {
        LoadResult a = client(1).load(new LoadRequest("load-a",
                List.of(row(1, 10, "a1"), row(2, 20, "a2"))));
        LoadResult b = client(1).load(new LoadRequest("load-b",
                List.of(row(3, 110, "b1"), row(4, 120, "b2"))));
        assertEquals(LoadState.STAGED, a.state());
        assertEquals(LoadState.STAGED, b.state());
        long snapshotsBefore = snapshotCount();
        long sBefore = catalog.readCutline(table).snapshot().id();

        new LoadAdoptionWorker(catalog, lake)
                .runCycle(catalog.get(table).orElseThrow());

        assertEquals(snapshotsBefore + 1, snapshotCount(),
                "all staged labels fold into one lake commit");
        assertTrue(catalog.readCutline(table).snapshot().id() > sBefore, "S advanced");
        assertEquals(new TierKey(200), catalog.readCutline(table).t(), "T untouched");
        assertTrue(lakeRows().containsAll(
                List.of("1|10|a1", "2|20|a2", "3|110|b1", "4|120|b2")));
        assertTrue(catalog.stagedLoads(table).isEmpty());
        assertEquals(LoadState.COMMITTED,
                catalog.lookupLoad(table, "load-a").orElseThrow().state());
        assertEquals(LoadState.COMMITTED,
                catalog.lookupLoad(table, "load-b").orElseThrow().state());
        assertEquals(1, countOps("load", OpPhase.ADVANCED));
    }

    @Test
    void adoptionIsANoOpWithNothingStaged() {
        long snapshotsBefore = snapshotCount();
        new LoadAdoptionWorker(catalog, lake)
                .runCycle(catalog.get(table).orElseThrow());
        assertEquals(snapshotsBefore, snapshotCount());
    }

    @Test
    void aCommittedButUnpublishedAdoptionResumesFromTheSnapshotStamps() throws Exception {
        client(1).load(new LoadRequest("crash-load",
                List.of(row(7, 30, "x"), row(8, 40, "y"))));

        // First adoption: commit lands, then the "crash": rewind the catalog
        // to the pre-publish state (label staged, S behind, op at 'committed').
        RegisteredTable meta = catalog.get(table).orElseThrow();
        new LoadAdoptionWorker(catalog, lake).runCycle(meta);
        long committedS = catalog.readCutline(table).snapshot().id();
        exec("UPDATE modak.cutline SET lake_snapshot_id = 0 WHERE table_id = " + table.oid());
        exec("UPDATE modak.load_labels SET state = 'staged' WHERE table_id = " + table.oid());
        exec("UPDATE modak.tiering_log SET phase = 'committed' "
                + "WHERE table_id = " + table.oid() + " AND op_kind = 'load'");
        long snapshotsBefore = snapshotCount();

        new LoadAdoptionWorker(catalog, lake).runCycle(meta);

        assertEquals(snapshotsBefore, snapshotCount(),
                "resume publishes the existing commit, it never re-commits");
        assertEquals(committedS, catalog.readCutline(table).snapshot().id());
        assertEquals(LoadState.COMMITTED,
                catalog.lookupLoad(table, "crash-load").orElseThrow().state());
        assertEquals(0, countOps("load", OpPhase.COMMITTED));
    }

    @Test
    void anUncommittedOpIsAbandonedAndItsLabelsReAdopted() throws Exception {
        client(1).load(new LoadRequest("retry-load",
                List.of(row(9, 50, "z1"), row(10, 60, "z2"))));

        // A crash before the lake commit: the op journal says flushing, the
        // label is still staged, and the files are still there.
        exec("INSERT INTO modak.tiering_log (op_id, table_id, op_kind, phase, details) VALUES "
                + "(gen_random_uuid(), " + table.oid()
                + ", 'load', 'flushing', '{\"labels\":[\"retry-load\"],\"files\":1}')");

        new LoadAdoptionWorker(catalog, lake)
                .runCycle(catalog.get(table).orElseThrow());

        assertEquals(1, countOps("load", OpPhase.ABANDONED));
        assertEquals(LoadState.COMMITTED,
                catalog.lookupLoad(table, "retry-load").orElseThrow().state());
        assertTrue(lakeRows().containsAll(List.of("9|50|z1", "10|60|z2")));
    }

    @Test
    void retentionNeverRisesPastAStagedLoad() {
        catalog.beginLoad(table, "pinned-low", LoadState.STAGED,
                "{\"files\":[\"/nowhere/a.parquet\"],\"lo\":30,\"hi\":40}", null);

        // heapFrontier is MAX (no partitions), lag 0 puts the raw boundary at
        // T=200, the staged load at lo=30 must cap it at 0 (floor to width 100).
        exec("UPDATE modak.tables SET lake_retention_lag = 0 WHERE table_id = " + table.oid());
        new RetentionWorker(catalog, lake).runCycle(catalog.get(table).orElseThrow());

        long line = catalog.readRetentionLine(table)
                .map(TierKey::value).orElse(Long.MIN_VALUE);
        assertTrue(line <= 30, "retention line " + line + " must stay at or below staged lo=30");
    }

    private long snapshotCount() {
        icebergTable.refresh();
        long n = 0;
        for (var ignored : icebergTable.snapshots()) {
            n++;
        }
        return n;
    }

    private List<String> lakeRows() throws IOException {
        icebergTable.refresh();
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(icebergTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("event_time")
                        + "|" + r.getField("val"));
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private long countOps(String kind, OpPhase phase) {
        return Long.parseLong(queryOne("SELECT count(*)::text FROM modak.tiering_log "
                + "WHERE table_id = " + table.oid() + " AND op_kind = '" + kind
                + "' AND phase = '" + phase.sql() + "'"));
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
