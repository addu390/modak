package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

class MetricsTest {

    @Test
    void rendersGaugesAndCountersInExpositionFormat() {
        Metrics metrics = new Metrics();
        metrics.gauge("modak_leader", 1);
        metrics.gauge(Metrics.series("modak_delta_backlog_rows", "table", "public.events"), 42);
        metrics.increment(Metrics.series("modak_lake_commits_total", "table", "public.events"));
        metrics.increment(Metrics.series("modak_lake_commits_total", "table", "public.events"));

        String out = metrics.render();
        assertTrue(out.contains("modak_leader 1\n"), out);
        assertTrue(out.contains("modak_delta_backlog_rows{table=\"public.events\"} 42\n"), out);
        assertTrue(out.contains("modak_lake_commits_total{table=\"public.events\"} 2\n"), out);
    }

    @Test
    void seriesEscapesLabelValues() {
        assertEquals("m{slot=\"a\\\"b\"}", Metrics.series("m", "slot", "a\"b"));
    }

    @Test
    void servesTheScrapeEndpoint() throws Exception {
        Metrics metrics = new Metrics();
        metrics.gauge("modak_leader", 0);
        MetricsServer server = MetricsServer.start(0, metrics, null);
        try {
            String body = get("http://localhost:" + server.port() + "/metrics");
            assertTrue(body.contains("modak_leader 0"), body);
        } finally {
            server.stop();
        }
    }

    private static String get(String url) throws IOException, InterruptedException {
        return HttpClient.newHttpClient()
                .send(HttpRequest.newBuilder(URI.create(url)).GET().build(),
                        HttpResponse.BodyHandlers.ofString())
                .body();
    }
}
