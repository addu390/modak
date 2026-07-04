package io.modak.worker;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * The headless worker's HTTP surface: {@code GET /metrics} on the configured
 * port (unset/zero = in-process only), plus Stream Load when a token is
 * configured. The console binary serves both itself and disables this one.
 */
final class MetricsServer {

    private final HttpServer server;

    private MetricsServer(HttpServer server) {
        this.server = server;
    }

    static MetricsServer start(int port, Metrics metrics, LoadEndpoint load) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = metrics.render().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type",
                    "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        if (load != null) {
            server.createContext(LoadEndpoint.PATH, load);
        }
        server.start();
        return new MetricsServer(server);
    }

    int port() {
        return server.getAddress().getPort();
    }

    void stop() {
        server.stop(0);
    }
}
