package io.modak.load;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.LoadLabel;
import io.modak.catalog.LoadState;
import io.modak.catalog.TableMode;
import io.modak.catalog.TableRegistration;
import io.modak.common.LakeSnapshotId;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@link LoadClient} against embedded Postgres, fake lake storage for the spool path. */
class LoadClientTest {

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;

    private JdbcCatalog catalog;
    private TableId table;

    @BeforeAll
    static void startPostgres() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);
    }

    @AfterAll
    static void stopPostgres() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @BeforeEach
    void reset() {
        exec("TRUNCATE modak.tables CASCADE");
        exec("DROP TABLE IF EXISTS public.events");
        // Partitioned like a real tiered table: no unique constraint on id,
        // the primary key is logical (declared at registration).
        exec("CREATE TABLE public.events (id bigint NOT NULL, ts bigint NOT NULL, v text) "
                + "PARTITION BY RANGE (ts)");
        exec("CREATE TABLE public.events_p0 PARTITION OF public.events "
                + "FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.events_p1 PARTITION OF public.events "
                + "FOR VALUES FROM (100) TO (200)");
        exec("CREATE TABLE public.events_p2 PARTITION OF public.events "
                + "FOR VALUES FROM (200) TO (400)");
        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                42L, "public", "events", List.of("id"), "ts",
                "{\"unit\":\"hour\"}", "iceberg", "warehouse.public.events", null));
        catalog.initCutline(table, new TierKey(100), new LakeSnapshotId(1));
    }

    private static LoadOptions options(int spoolThreshold) {
        return LoadOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.events")
                .spoolThreshold(spoolThreshold)
                .build();
    }

    private static Map<String, Object> row(long id, long ts, String v) {
        return Map.of("id", id, "ts", ts, "v", v);
    }

    @Test
    void hotRowsLandInTheHeapAndTheLabelCommits() {
        LoadResult r = new LoadClient(options(1000)).load(new LoadRequest("b1",
                List.of(row(1, 150, "a"), row(2, 200, "b"))));

        assertEquals(LoadState.COMMITTED, r.state());
        assertEquals(2, r.hotRows());
        assertEquals(0, r.deltaRows());
        assertFalse(r.replay());
        assertEquals(2, countHeap());
        assertEquals(LoadState.COMMITTED,
                catalog.lookupLoad(table, "b1").orElseThrow().state());
    }

    @Test
    void aStraddlingBatchTricklesColdRowsIntoDelta() {
        LoadResult r = new LoadClient(options(1000)).load(new LoadRequest("b2",
                List.of(row(1, 150, "hot"), row(2, 40, "cold"), row(3, 60, "cold"))));

        assertEquals(1, r.hotRows());
        assertEquals(2, r.deltaRows());
        assertEquals(1, countHeap());
        assertEquals(2, queryLong("SELECT count(*) FROM modak.delta"));
        assertEquals("2", queryText(
                "SELECT pk FROM modak.delta WHERE tier_key = 40"));
    }

    @Test
    void replayingAFinishedLabelReturnsTheRecordedResultWithoutReapplying() {
        LoadClient client = new LoadClient(options(1000));
        LoadResult first = client.load(new LoadRequest("b3", List.of(row(1, 150, "a"))));

        // Same label, different rows: nothing new may land.
        LoadResult second = client.load(new LoadRequest("b3",
                List.of(row(8, 150, "x"), row(9, 150, "y"))));

        assertTrue(second.replay());
        assertEquals(first.hotRows(), second.hotRows());
        assertEquals(1, countHeap(), "the replayed batch was not applied");
    }

    @Test
    void reloadingTheSamePkUnderANewLabelUpsertsInsteadOfDuplicating() {
        LoadClient client = new LoadClient(options(1000));
        client.load(new LoadRequest("b4", List.of(row(1, 150, "old"))));
        client.load(new LoadRequest("b5", List.of(row(1, 150, "new"))));

        assertEquals(1, countHeap());
        assertEquals("new", queryText("SELECT v FROM public.events WHERE id = 1"));
    }

    @Test
    void coldVolumeSpoolsAsStagedParquetAndRecordsAStagedLabel() {
        FakeLakeStorage lake = new FakeLakeStorage();
        LoadResult r = new LoadClient(options(2), format -> lake).load(new LoadRequest("b6",
                List.of(row(1, 10, "a"), row(2, 20, "b"), row(3, 30, "c"), row(4, 150, "hot"))));

        assertEquals(LoadState.STAGED, r.state());
        assertEquals(1, r.hotRows());
        assertEquals(3, r.spooledRows());
        assertEquals(List.of("/fake/warehouse/staged-1.parquet"), r.stagedFiles());
        assertEquals(3, lake.stagedRows.size());
        assertEquals(0, queryLong("SELECT count(*) FROM modak.delta"));

        LoadLabel label = catalog.lookupLoad(table, "b6").orElseThrow();
        assertEquals(LoadState.STAGED, label.state());
        assertTrue(label.stagedFilesJson().contains("staged-1.parquet"));
        assertTrue(label.stagedFilesJson().contains("\"lo\": 10"), label.stagedFilesJson());
        assertTrue(label.stagedFilesJson().contains("\"hi\": 30"), label.stagedFilesJson());
        assertEquals(List.of("b6"),
                catalog.stagedLoads(table).stream().map(LoadLabel::label).toList());
    }

    @Test
    void aRowBelowTheRetentionLineRejectsAndRecordsNothing() {
        exec("UPDATE modak.cutline SET retention_line = 50 WHERE table_id = 42");

        assertThrows(LoadException.class, () -> new LoadClient(options(1000))
                .load(new LoadRequest("b7", List.of(row(1, 150, "ok"), row(2, 10, "expired")))));

        assertEquals(Optional.empty(), catalog.lookupLoad(table, "b7"),
                "a rejected batch leaves no label, the client may fix and retry it");
        assertEquals(0, countHeap());
    }

    @Test
    void fullyMirroredTablesTakeEverythingOnTheHeap() {
        exec("DROP TABLE IF EXISTS public.vehicles");
        exec("CREATE TABLE public.vehicles (id bigint PRIMARY KEY, ts bigint NOT NULL, v text)");
        TableId mirrored = catalog.register(new TableRegistration(
                77L, "public", "vehicles", List.of("id"), "ts",
                "{}", "iceberg", "warehouse.public.vehicles", null,
                TableMode.MIRRORED, "pub", "slot", Optional.empty(), Optional.empty()));
        catalog.initCutline(mirrored, new TierKey(1000), new LakeSnapshotId(1));

        LoadOptions opts = LoadOptions.builder()
                .jdbcUrl(postgres.getJdbcUrl("postgres", "postgres"))
                .table("public.vehicles")
                .build();
        LoadResult r = new LoadClient(opts).load(new LoadRequest("b8",
                List.of(row(1, 5, "ancient"), row(2, 5000, "fresh"))));

        assertEquals(2, r.hotRows());
        assertEquals(0, r.deltaRows());
        assertEquals(2, queryLong("SELECT count(*) FROM public.vehicles"));
    }

    @Test
    void anEmptyBatchStillCommitsItsLabel() {
        LoadResult r = new LoadClient(options(1000)).load(new LoadRequest("b9", List.of()));
        assertEquals(LoadState.COMMITTED, r.state());
        assertEquals(0, r.hotRows() + r.deltaRows() + r.spooledRows());
        assertTrue(catalog.lookupLoad(table, "b9").isPresent());
    }

    @Test
    void columnsAbsentFromTheBatchStayNull() {
        new LoadClient(options(1000)).load(new LoadRequest("b10",
                List.of(Map.of("id", 1L, "ts", 150L))));
        assertEquals(1, countHeap());
        assertEquals(null, queryText("SELECT v FROM public.events WHERE id = 1"));
    }

    private long countHeap() {
        return queryLong("SELECT count(*) FROM public.events");
    }

    private static long queryLong(String sql) {
        try (Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String queryText(String sql) {
        try (Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
