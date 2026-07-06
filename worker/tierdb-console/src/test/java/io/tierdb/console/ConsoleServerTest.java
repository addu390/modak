package io.tierdb.console;

import io.tierdb.worker.http.Metrics;
import io.tierdb.worker.http.SeriesStore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
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
                "{\"unit\":\"range\"}", "iceberg", "/wh/events"));
        catalog.initCutline(table, new TierKey(100), new LakeSnapshotId(7));
        exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version) VALUES ("
                + table.oid() + ", '42', 0, 50, 1)");
        exec("INSERT INTO tierdb.partitions (table_id, partition_id, tier_key_lo, tier_key_hi, "
                + "state) VALUES (" + table.oid() + ", 'events_p0', 0, 100, 'tiered')");
        exec("INSERT INTO tierdb.op_log (op_id, table_id, op_kind, phase, lake_snapshot_id, "
                + "details) VALUES (gen_random_uuid(), " + table.oid()
                + ", 'tiering', 'advanced', 7, '{\"rows_written\": 10}')");

        SeriesStore series = new SeriesStore();
        series.record("delta_backlog|public.events", 1);
        Metrics metrics = new Metrics();
        metrics.gauge("tierdb_leader", 1);
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
        var res = get("/api/v1/overview");
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
        var res = get("/api/v1/tables/" + table.oid());
        assertEquals(200, res.statusCode());
        String body = res.body();
        assertTrue(body.contains("\"pk\":[\"id\"]"), body);
        assertTrue(body.contains("\"id\":\"events_p0\""), body);
        assertTrue(body.contains("\"phase\":\"advanced\""), body);
        assertTrue(body.contains("rows_written"), body);
    }

    @Test
    void unknownTableIs404() throws Exception {
        assertEquals(404, get("/api/v1/tables/999999").statusCode());
        assertEquals(404, get("/api/v1/tables").statusCode());
        assertEquals(404, get("/api/v1/tables/nope").statusCode());
    }

    @Test
    void maintenanceRequestIsFiledAndReflected() throws Exception {
        var res = post("/api/v1/tables/" + table.oid() + "/maintenance", "");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("\"requested\":true"), res.body());

        var detail = get("/api/v1/tables/" + table.oid());
        assertTrue(detail.body().contains("\"requestedBy\":\"console\""), detail.body());

        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT requested_by "
                        + "FROM tierdb.maintenance_requests WHERE table_id = " + table.oid())) {
            assertTrue(rs.next());
            assertEquals("console", rs.getString(1));
        }
        exec("DELETE FROM tierdb.maintenance_requests WHERE table_id = " + table.oid());

        assertEquals(404, post("/api/v1/tables/999999/maintenance", "").statusCode());
        assertEquals(405, get("/api/v1/tables/" + table.oid() + "/maintenance").statusCode());
    }

    @Test
    void storageProfilesListAndCreate() throws Exception {
        var list = get("/api/v1/storage-profiles");
        assertEquals(200, list.statusCode());
        assertTrue(list.body().contains("\"name\":\"default\""), list.body());
        assertTrue(list.body().contains("\"isDefault\":true"), list.body());

        var created = post("/api/v1/storage-profiles", """
                {"name":"gcs-eu","warehouse":"gs://analytics-eu","lakeFormat":"iceberg",
                 "lakeConfig":{"gcs.project-id":"acme-eu"},"credentialRef":"analytics"}
                """);
        assertEquals(200, created.statusCode());
        assertTrue(created.body().contains("\"created\":true"), created.body());

        var again = get("/api/v1/storage-profiles");
        assertTrue(again.body().contains("\"name\":\"gcs-eu\""), again.body());
        assertTrue(again.body().contains("\"warehouse\":\"gs://analytics-eu\""), again.body());
        assertTrue(again.body().contains("\"gcs.project-id\": \"acme-eu\"")
                || again.body().contains("\"gcs.project-id\":\"acme-eu\""), again.body());
        assertTrue(again.body().contains("\"credentialRef\":\"analytics\""), again.body());

        assertEquals(409, post("/api/v1/storage-profiles",
                "{\"name\":\"gcs-eu\",\"warehouse\":\"gs://x\"}").statusCode());
        assertEquals(400, post("/api/v1/storage-profiles",
                "{\"warehouse\":\"gs://x\"}").statusCode());
        assertEquals(400, post("/api/v1/storage-profiles", "not json").statusCode());
        exec("DELETE FROM tierdb.storage_profiles WHERE profile_name = 'gcs-eu'");
    }

    @Test
    void tableDetailNamesItsStorageProfile() throws Exception {
        var res = get("/api/v1/tables/" + table.oid());
        assertTrue(res.body().contains("\"storageProfile\":\"default\""), res.body());
    }

    @Test
    void servesTheOpenApiSpec() throws Exception {
        var res = get("/api/openapi.yaml");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("openapi: 3"), res.body());
        assertTrue(res.body().contains("/api/v1/tables/{tableId}/maintenance"), res.body());
    }

    @Test
    void seriesServesRecordedPoints() throws Exception {
        var res = get("/api/v1/series");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("\"delta_backlog|public.events\":[["), res.body());
    }

    @Test
    void servesConsoleAssetsAndMetrics() throws Exception {
        var index = get("/");
        assertEquals(200, index.statusCode());
        assertTrue(index.body().contains("TierDB Console"), "index.html should be served");
        assertEquals(200, get("/app.js").statusCode());
        assertEquals(200, get("/style.css").statusCode());
        assertEquals(200, get("/vendor/echarts.min.js").statusCode());
        assertEquals(200, get("/vendor/codemirror.js").statusCode());
        assertEquals(200, get("/vendor/codemirror.css").statusCode());
        assertEquals(200, get("/vendor/codemirror-sql.js").statusCode());
        assertEquals(404, get("/vendor/nope.js").statusCode());
        assertEquals(404, get("/nope.html").statusCode());
        assertTrue(get("/metrics").body().contains("tierdb_leader 1"));
    }

    @Test
    void playgroundRunsQueriesAndDdl() throws Exception {
        var select = post("/api/v1/query", "SELECT 1 AS one, 'x' AS s, NULL AS n");
        assertEquals(200, select.statusCode());
        assertTrue(select.body().contains("\"columns\":[{\"name\":\"one\""), select.body());
        assertTrue(select.body().contains("\"rows\":[[\"1\",\"x\",null]]"), select.body());

        var ddl = post("/api/v1/query", "CREATE TABLE public.scratch (id int)");
        assertTrue(ddl.body().contains("\"updateCount\""), ddl.body());
        var insert = post("/api/v1/query", "INSERT INTO public.scratch SELECT 1");
        assertTrue(insert.body().contains("\"updateCount\":1"), insert.body());

        var big = post("/api/v1/query", "SELECT * FROM generate_series(1, 2000)");
        assertTrue(big.body().contains("\"rowCount\":1000"), big.body());
        assertTrue(big.body().contains("\"truncated\":true"), big.body());

        var bad = post("/api/v1/query", "SELECT * FROM does_not_exist");
        assertEquals(400, bad.statusCode());
        assertTrue(bad.body().contains("\"error\":"), bad.body());

        var empty = post("/api/v1/query", "   ");
        assertEquals(400, empty.statusCode());
        assertTrue(empty.body().contains("empty statement"), empty.body());

        assertEquals(405, get("/api/v1/query").statusCode());
    }

    @Test
    void schemaListsTablesAndColumns() throws Exception {
        var res = get("/api/v1/schema");
        assertEquals(200, res.statusCode());
        assertTrue(res.body().contains("\"name\":\"public.events\""), res.body());
        assertTrue(res.body().contains("\"name\":\"tierdb.tables\""), res.body());
    }

    @Test
    void queryEndpointCanBeDisabled() throws Exception {
        ConsoleServer locked = ConsoleServer.start(0, new Metrics(),
                new ConsoleData(dataSource), new SeriesStore(), () -> true, null, null);
        try {
            var res = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + locked.port()
                            + "/api/v1/query")).POST(HttpRequest.BodyPublishers.ofString(
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
