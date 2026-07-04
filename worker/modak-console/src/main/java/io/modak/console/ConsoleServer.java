package io.modak.console;

import io.modak.worker.Metrics;
import io.modak.worker.SeriesStore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.function.BooleanSupplier;

/**
 * The worker's HTTP face, the embedded console (static assets plus JSON API)
 * and the Prometheus scrape endpoint, on one port via the JDK's built-in
 * server. Read-only and unauthenticated, keep it on an internal port.
 */
final class ConsoleServer {

    private final HttpServer server;

    private ConsoleServer(HttpServer server) {
        this.server = server;
    }

    static ConsoleServer start(int port, Metrics metrics, ConsoleData data,
            SeriesStore series, BooleanSupplier leading, ConsoleQuery playground,
            HttpHandler load) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        if (load != null) {
            server.createContext("/api/load", load);
        }
        server.createContext("/metrics", exchange -> send(exchange, 200,
                "text/plain; version=0.0.4; charset=utf-8", metrics.render()));
        server.createContext("/api/overview", exchange ->
                json(exchange, () -> data.overview(leading.getAsBoolean())));
        server.createContext("/api/table", exchange -> json(exchange, () -> {
            Long id = queryId(exchange);
            String body = id == null ? null : data.table(id);
            if (body == null) {
                throw new NotFound();
            }
            return body;
        }));
        server.createContext("/api/series", exchange ->
                json(exchange, () -> seriesJson(series)));
        server.createContext("/api/schema", exchange -> {
            if (playground == null) {
                send(exchange, 403, "application/json", "{\"error\":\"SQL disabled\"}");
                return;
            }
            json(exchange, playground::schema);
        });
        server.createContext("/api/query", exchange -> {
            if (playground == null) {
                send(exchange, 403, "application/json",
                        "{\"error\":\"SQL disabled (MODAK_CONSOLE_SQL=false)\"}");
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                send(exchange, 405, "application/json", "{\"error\":\"POST only\"}");
                return;
            }
            json(exchange, () -> {
                String sql = new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8).strip();
                if (sql.isEmpty()) {
                    return "{\"error\":\"empty statement\"}";
                }
                return playground.run(sql);
            });
        });
        server.createContext("/api/explain", exchange -> {
            if (playground == null) {
                send(exchange, 403, "application/json",
                        "{\"error\":\"SQL disabled (MODAK_CONSOLE_SQL=false)\"}");
                return;
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                send(exchange, 405, "application/json", "{\"error\":\"POST only\"}");
                return;
            }
            json(exchange, () -> {
                String sql = new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8).strip();
                if (sql.isEmpty()) {
                    return "{\"error\":\"empty statement\"}";
                }
                return playground.explain(sql);
            });
        });
        server.createContext("/", ConsoleServer::asset);
        server.start();
        return new ConsoleServer(server);
    }

    int port() {
        return server.getAddress().getPort();
    }

    void stop() {
        server.stop(0);
    }

    private static final class NotFound extends Exception {}

    private interface Body {
        String get() throws Exception;
    }

    private static void json(HttpExchange exchange, Body body) {
        try {
            send(exchange, 200, "application/json", body.get());
        } catch (NotFound e) {
            send(exchange, 404, "application/json", "{\"error\":\"not found\"}");
        } catch (Exception e) {
            Log.error("console api %s failed: %s", exchange.getRequestURI().getPath(), e);
            send(exchange, 500, "application/json",
                    "{\"error\":" + Json.str(String.valueOf(e)) + "}");
        }
    }

    private static String seriesJson(SeriesStore series) {
        StringJoiner out = new StringJoiner(",", "{", "}");
        for (Map.Entry<String, double[][]> e : series.snapshot().entrySet()) {
            StringJoiner points = new StringJoiner(",", "[", "]");
            for (double[] p : e.getValue()) {
                points.add("[" + (long) p[0] + "," + Json.num(p[1]) + "]");
            }
            out.add(Json.str(e.getKey()) + ":" + points);
        }
        return "{\"series\":" + out + "}";
    }

    private static Long queryId(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            if (pair.startsWith("id=")) {
                try {
                    return Long.parseLong(pair.substring(3));
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return null;
    }

    private static final Map<String, String> VENDOR = vendorAssets();

    private static Map<String, String> vendorAssets() {
        String echarts = webjarVersion("echarts");
        String codemirror = webjarVersion("codemirror");
        return Map.of(
                "/vendor/echarts.min.js",
                "/META-INF/resources/webjars/echarts/" + echarts + "/dist/echarts.min.js",
                "/vendor/codemirror.js",
                "/META-INF/resources/webjars/codemirror/" + codemirror + "/lib/codemirror.js",
                "/vendor/codemirror.css",
                "/META-INF/resources/webjars/codemirror/" + codemirror + "/lib/codemirror.css",
                "/vendor/codemirror-sql.js",
                "/META-INF/resources/webjars/codemirror/" + codemirror + "/mode/sql/sql.js");
    }

    private static String webjarVersion(String artifact) {
        String path = "/META-INF/maven/org.webjars.npm/" + artifact + "/pom.properties";
        try (InputStream in = ConsoleServer.class.getResourceAsStream(path)) {
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("version");
        } catch (Exception e) {
            throw new IllegalStateException("webjar " + artifact + " missing from classpath", e);
        }
    }

    private static void asset(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath();
        if (path.equals("/")) {
            path = "/index.html";
        }
        if (path.contains("..")) {
            send(exchange, 404, "text/plain", "not found");
            return;
        }
        String resource = path.startsWith("/vendor/")
                ? VENDOR.get(path)
                : "/console" + path;
        if (resource == null) {
            send(exchange, 404, "text/plain", "not found");
            return;
        }
        try (InputStream in = ConsoleServer.class.getResourceAsStream(resource)) {
            if (in == null) {
                send(exchange, 404, "text/plain", "not found");
                return;
            }
            byte[] body = in.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", contentType(path));
            exchange.getResponseHeaders().set("Cache-Control",
                    path.startsWith("/vendor/") ? "max-age=86400" : "no-cache");
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        } catch (Exception e) {
            Log.error("console asset %s failed: %s", path, e);
        }
    }

    private static String contentType(String path) {
        if (path.endsWith(".html")) {
            return "text/html; charset=utf-8";
        }
        if (path.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (path.endsWith(".js")) {
            return "application/javascript; charset=utf-8";
        }
        if (path.endsWith(".svg")) {
            return "image/svg+xml";
        }
        return "application/octet-stream";
    }

    private static void send(HttpExchange exchange, int status, String contentType,
            String body) {
        try {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", contentType);
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        } catch (Exception e) {
            Log.error("console response failed: %s", e);
        }
    }
}
