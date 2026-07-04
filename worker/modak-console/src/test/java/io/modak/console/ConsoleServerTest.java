package io.modak.console;

import io.modak.worker.Metrics;
import io.modak.worker.SeriesStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.TableRegistration;
import io.modak.common.LakeSnapshotId;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The console's JSON API and static assets against a real catalog. */
class ConsoleServerTest {

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static TableId table;
    private static ConsoleServer server;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);

        exec("CREATE TABLE public.events (id bigint PRIMARY KEY, event_time bigint, val text)");
        JdbcCatalog catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                relOid("public.events"), "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"range\"}", "iceberg", "/wh/events", null));
        catalog.initCutline(table, new TierKey(100), new LakeSnapshotId(7));
        exec("INSERT INTO modak.delta (table_id, pk, op, tier_key, version) VALUES ("
                + table.oid() + ", '42', 0, 50, 1)");
        exec("INSERT INTO modak.partitions (table_id, partition_id, tier_key_lo, tier_key_hi, "
                + "state) VALUES (" + table.oid() + ", 'events_p0', 0, 100, 'tiered')");
        exec("INSERT INTO modak.tiering_log (op_id, table_id, op_kind, phase, lake_snapshot_id, "
                + "details) VALUES (gen_random_uuid(), " + table.oid()
                + ", 'tiering', 'advanced', 7, '{\"rows_written\": 10}')");

        SeriesStore series = new SeriesStore();
        series.record("delta_backlog|public.events", 1);
        Metrics metrics = new Metrics();
        metrics.gauge("modak_leader", 1);
        server = ConsoleServer.start(0, metrics, new ConsoleData(dataSource), series,
                () -> true, new ConsoleQuery(dataSource), null);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (server != null) {
            server.stop();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void overviewReportsTheFleet() throws Exception {
        var res = get("/api/overview");
        assertEquals(200, res.statusCode());
        String body = res.body();
        assertTrue(body.contains("\"leader\":true"), body);
        assertTrue(body.contains("\"name\":\"events\""), body);
        assertTrue(body.contains("\"cutlineT\":100"), body);
        assertTrue(body.contains("\"cutlineS\":7"), body);
        assertTrue(body.contains("\"deltaBacklog\":1"), body);
        assertTrue(body.contains("\"tiered\": 1") || body.contains("\"tiered\":1"), body);
    }

    @Test
    void tableDetailIncludesPartitionsAndOps() throws Exception {
        var res = get("/api/table?id=" + table.oid());
        assertEquals(200, res.statusCode());
        String body = res.body();
        assertTrue(body.contains("\"pk\":[\"id\"]"), body);
        assertTrue(body.contains("\"id\":\"events_p0\""), body);
        assertTrue(body.contains("\"phase\":\"advanced\""), body);
        assertTrue(body.contains("rows_written"), body);
    }

    @Test
    void unknownTableIs404() throws Exception {
        assertEquals(404, get("/api/table?id=999999").statusCode());
        assertEquals(404, get("/api/table").statusCode());
    }

    @Test
    void seriesServesRecordedPoints() throws Exception {
        var res = get("/api/series");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("\"delta_backlog|public.events\":[["), res.body());
    }

    @Test
    void servesConsoleAssetsAndMetrics() throws Exception {
        var index = get("/");
        assertEquals(200, index.statusCode());
        assertTrue(index.body().contains("Modak Console"), "index.html should be served");
        assertEquals(200, get("/app.js").statusCode());
        assertEquals(200, get("/style.css").statusCode());
        assertEquals(200, get("/vendor/echarts.min.js").statusCode());
        assertEquals(200, get("/vendor/codemirror.js").statusCode());
        assertEquals(200, get("/vendor/codemirror.css").statusCode());
        assertEquals(200, get("/vendor/codemirror-sql.js").statusCode());
        assertEquals(404, get("/vendor/nope.js").statusCode());
        assertEquals(404, get("/nope.html").statusCode());
        assertTrue(get("/metrics").body().contains("modak_leader 1"));
    }

    @Test
    void playgroundRunsQueriesAndDdl() throws Exception {
        var select = post("/api/query", "SELECT 1 AS one, 'x' AS s, NULL AS n");
        assertEquals(200, select.statusCode());
        assertTrue(select.body().contains("\"columns\":[{\"name\":\"one\""), select.body());
        assertTrue(select.body().contains("\"rows\":[[\"1\",\"x\",null]]"), select.body());

        var ddl = post("/api/query", "CREATE TABLE public.scratch (id int)");
        assertTrue(ddl.body().contains("\"updateCount\""), ddl.body());
        var insert = post("/api/query", "INSERT INTO public.scratch SELECT 1");
        assertTrue(insert.body().contains("\"updateCount\":1"), insert.body());

        var big = post("/api/query", "SELECT * FROM generate_series(1, 2000)");
        assertTrue(big.body().contains("\"rowCount\":1000"), big.body());
        assertTrue(big.body().contains("\"truncated\":true"), big.body());

        var bad = post("/api/query", "SELECT * FROM does_not_exist");
        assertEquals(200, bad.statusCode());
        assertTrue(bad.body().contains("\"error\":"), bad.body());

        assertEquals(405, get("/api/query").statusCode());
    }

    @Test
    void schemaListsTablesAndColumns() throws Exception {
        var res = get("/api/schema");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("\"name\":\"public.events\""), res.body());
        assertTrue(res.body().contains("\"name\":\"modak.tables\""), res.body());
    }

    @Test
    void queryEndpointCanBeDisabled() throws Exception {
        ConsoleServer locked = ConsoleServer.start(0, new Metrics(),
                new ConsoleData(dataSource), new SeriesStore(), () -> true, null, null);
        try {
            var res = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + locked.port()
                            + "/api/query")).POST(HttpRequest.BodyPublishers.ofString(
                                    "SELECT 1")).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(403, res.statusCode());
        } finally {
            locked.stop();
        }
    }

    private static HttpResponse<String> post(String path, String body) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + server.port() + path))
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(String path) throws Exception {
        return HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(
                        "http://localhost:" + server.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static void exec(String sql) throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        }
    }

    private static long relOid(String rel) throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT '" + rel + "'::regclass::oid")) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
