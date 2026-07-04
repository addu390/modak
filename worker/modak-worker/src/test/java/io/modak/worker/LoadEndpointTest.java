package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.TableRegistration;
import io.modak.common.LakeSnapshotId;
import io.modak.common.TierKey;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The Stream Load HTTP surface: token gate, path/label validation, JSONL in, JSON out. */
class LoadEndpointTest {

    private static final String TOKEN = "sesame";

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static MetricsServer server;
    private static Metrics metrics;

    @BeforeAll
    static void setUp() throws Exception {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);

        // Partitioned like a real tiered table: the primary key is logical.
        exec("CREATE TABLE public.events (id bigint NOT NULL, ts bigint NOT NULL, v text) "
                + "PARTITION BY RANGE (ts)");
        exec("CREATE TABLE public.events_p0 PARTITION OF public.events "
                + "FOR VALUES FROM (0) TO (200)");
        exec("CREATE TABLE public.events_p1 PARTITION OF public.events "
                + "FOR VALUES FROM (200) TO (400)");
        JdbcCatalog catalog = new JdbcCatalog(dataSource);
        var table = catalog.register(new TableRegistration(
                42L, "public", "events", List.of("id"), "ts",
                "{\"unit\":\"hour\"}", "iceberg", "warehouse.public.events", null));
        catalog.initCutline(table, new TierKey(100), new LakeSnapshotId(1));

        WorkerConfig config = WorkerConfig.fromEnv(Map.of(
                "MODAK_PG_URL", postgres.getJdbcUrl("postgres", "postgres"),
                "MODAK_PG_USER", "postgres",
                "MODAK_LOAD_TOKEN", TOKEN));
        metrics = new Metrics();
        server = MetricsServer.start(0, metrics, LoadEndpoint.fromConfig(config, metrics));
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (server != null) {
            server.stop();
        }
        if (postgres != null) {
            postgres.close();
        }
    }

    @BeforeEach
    void reset() {
        exec("TRUNCATE public.events");
        exec("DELETE FROM modak.delta");
        exec("DELETE FROM modak.load_labels");
    }

    @Test
    void anUnsetTokenDisablesTheEndpoint() {
        WorkerConfig config = WorkerConfig.fromEnv(Map.of("MODAK_PG_URL", "jdbc:x"));
        assertNull(LoadEndpoint.fromConfig(config, new Metrics()));
    }

    @Test
    void requestsWithoutTheTokenAreRejected() throws Exception {
        var res = post("/api/load/public.events", "t-noauth", null,
                "{\"id\":1,\"ts\":150,\"v\":\"a\"}");
        assertEquals(401, res.statusCode());

        var wrong = post("/api/load/public.events", "t-wrong", "not-it",
                "{\"id\":1,\"ts\":150,\"v\":\"a\"}");
        assertEquals(401, wrong.statusCode());
    }

    @Test
    void aLabeledJsonlBatchLandsAndReplaysById() throws Exception {
        var res = post("/api/load/public.events", "http-1", TOKEN,
                "{\"id\":1,\"ts\":150,\"v\":\"a\"}\n{\"id\":2,\"ts\":40,\"v\":\"cold\"}\n");
        assertEquals(200, res.statusCode(), res.body());
        assertTrue(res.body().contains("\"hot_rows\":1"), res.body());
        assertTrue(res.body().contains("\"delta_rows\":1"), res.body());
        assertTrue(res.body().contains("\"replay\":false"), res.body());
        assertEquals(1, queryLong("SELECT count(*) FROM public.events"));

        var replay = post("/api/load/public.events", "http-1", TOKEN,
                "{\"id\":9,\"ts\":150,\"v\":\"ignored\"}\n");
        assertEquals(200, replay.statusCode(), replay.body());
        assertTrue(replay.body().contains("\"replay\":true"), replay.body());
        assertEquals(1, queryLong("SELECT count(*) FROM public.events"),
                "the replayed batch was not applied");

        // Counters accumulate across tests, only presence is stable.
        String rendered = metrics.render();
        assertTrue(rendered.contains(
                "modak_load_total{table=\"public.events\",state=\"committed\"}"), rendered);
        assertTrue(rendered.contains(
                "modak_load_total{table=\"public.events\",state=\"replay\"}"), rendered);
        assertTrue(rendered.contains(
                "modak_load_rows_total{table=\"public.events\",path=\"heap\"}"), rendered);
    }

    @Test
    void aBearerTokenWorksToo() throws Exception {
        var res = HttpClient.newHttpClient().send(HttpRequest.newBuilder(url("/api/load/public.events"))
                .header("Authorization", "Bearer " + TOKEN)
                .header("X-Modak-Label", "http-bearer")
                .POST(HttpRequest.BodyPublishers.ofString("{\"id\":5,\"ts\":150,\"v\":\"b\"}"))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, res.statusCode(), res.body());
    }

    @Test
    void badRequestsGetClearErrors() throws Exception {
        assertEquals(400, post("/api/load/", "t-path", TOKEN, "{}").statusCode());
        assertEquals(400, post("/api/load/public.events", null, TOKEN,
                "{\"id\":1,\"ts\":150}").statusCode());

        var expired = post("/api/load/public.events", "t-expired", TOKEN,
                "{\"id\":1,\"ts\":150,\"v\":\"x\"}");
        exec("UPDATE modak.cutline SET retention_line = 50 WHERE table_id = 42");
        var rejected = post("/api/load/public.events", "t-below", TOKEN,
                "{\"id\":7,\"ts\":10,\"v\":\"old\"}");
        exec("UPDATE modak.cutline SET retention_line = NULL WHERE table_id = 42");
        assertEquals(200, expired.statusCode());
        assertEquals(400, rejected.statusCode(), rejected.body());
        assertTrue(rejected.body().contains("retention line"), rejected.body());

        var get = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(url("/api/load/public.events")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(405, get.statusCode());
    }

    private static URI url(String path) {
        return URI.create("http://localhost:" + server.port() + path);
    }

    private static HttpResponse<String> post(String path, String label, String token,
            String body) throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder(url(path))
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (label != null) {
            req.header("X-Modak-Label", label);
        }
        if (token != null) {
            req.header("X-Modak-Token", token);
        }
        return HttpClient.newHttpClient().send(req.build(), HttpResponse.BodyHandlers.ofString());
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

    private static void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
