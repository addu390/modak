package io.tierdb.console;

import io.tierdb.worker.http.Metrics;
import io.tierdb.worker.http.SeriesStore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
        server.createContext("/api/openapi.yaml", exchange ->
                sendResource(exchange, "/console/openapi.yaml", "application/yaml"));
        server.createContext("/api/v1/overview", exchange -> json(exchange, () -> {
            requireMethod(exchange, "GET");
            return data.overview(leading.getAsBoolean());
        }));
        server.createContext("/api/v1/tables", exchange -> json(exchange, () -> {
            String[] rest = subPath(exchange, "/api/v1/tables");
            long id = pathId(rest);
            if (rest.length == 1) {
                requireMethod(exchange, "GET");
                String body = data.table(id);
                if (body == null) {
                    throw new HttpError(404, "not found");
                }
                return body;
            }
            if (rest.length == 2 && rest[1].equals("maintenance")) {
                requireMethod(exchange, "POST");
                if (!data.requestMaintenance(id)) {
                    throw new HttpError(404, "not found");
                }
                return "{\"requested\":true}";
            }
            throw new HttpError(404, "not found");
        }));
        server.createContext("/api/v1/storage-profiles", exchange -> json(exchange, () -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                return data.storageProfiles();
            }
            requireMethod(exchange, "POST");
            return createProfile(exchange, data);
        }));
        server.createContext("/api/v1/series", exchange -> json(exchange, () -> {
            requireMethod(exchange, "GET");
            return seriesJson(series);
        }));
        server.createContext("/api/v1/schema", exchange -> json(exchange, () -> {
            requirePlayground(playground);
            requireMethod(exchange, "GET");
            return playground.schema();
        }));
        server.createContext("/api/v1/query", exchange -> json(exchange, () -> {
            requirePlayground(playground);
            requireMethod(exchange, "POST");
            return playground.run(requestSql(exchange));
        }));
        server.createContext("/api/v1/explain", exchange -> json(exchange, () -> {
            requirePlayground(playground);
            requireMethod(exchange, "POST");
            return playground.explain(requestSql(exchange));
        }));
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

    private interface Body {
        String get() throws Exception;
    }

    private static void json(HttpExchange exchange, Body body) {
        try {
            send(exchange, 200, "application/json", body.get());
        } catch (HttpError e) {
            send(exchange, e.status(), "application/json", e.bodyJson());
        } catch (Exception e) {
            Log.error("console api %s failed: %s", exchange.getRequestURI().getPath(), e);
            send(exchange, 500, "application/json",
                    "{\"error\":" + Json.str(String.valueOf(e)) + "}");
        }
    }

    private static void requireMethod(HttpExchange exchange, String method)
            throws HttpError {
        if (!method.equals(exchange.getRequestMethod())) {
            throw new HttpError(405, method + " only");
        }
    }

    private static void requirePlayground(ConsoleQuery playground) throws HttpError {
        if (playground == null) {
            throw new HttpError(403, "SQL disabled (TIERDB_CONSOLE_SQL=false)");
        }
    }

    private static String createProfile(HttpExchange exchange, ConsoleData data)
            throws Exception {
        JsonNode body;
        try {
            body = new ObjectMapper().readTree(exchange.getRequestBody());
        } catch (Exception e) {
            throw new HttpError(400, "body must be a JSON object");
        }
        String name = textOrNull(body, "name");
        String warehouse = textOrNull(body, "warehouse");
        if (name == null || warehouse == null) {
            throw new HttpError(400, "name and warehouse are required");
        }
        JsonNode config = body.get("lakeConfig");
        if (config != null && !config.isNull() && !config.isObject()) {
            throw new HttpError(400, "lakeConfig must be a JSON object of provider key=values");
        }
        try {
            data.createStorageProfile(name,
                    textOrNull(body, "lakeFormat"), warehouse,
                    config == null || config.isNull() ? null : config.toString(),
                    textOrNull(body, "credentialRef"),
                    body.path("isDefault").asBoolean(false));
        } catch (java.sql.SQLException e) {
            if ("23505".equals(e.getSQLState())) {
                throw new HttpError(409, "profile '" + name + "' already exists "
                        + "(or another default is set)");
            }
            throw e;
        }
        return "{\"created\":true}";
    }

    private static String textOrNull(JsonNode body, String field) {
        JsonNode node = body.get(field);
        return node == null || node.isNull() || node.asText().isBlank()
                ? null : node.asText();
    }

    private static String requestSql(HttpExchange exchange) throws Exception {
        String sql = new String(exchange.getRequestBody().readAllBytes(),
                StandardCharsets.UTF_8).strip();
        if (sql.isEmpty()) {
            throw new HttpError(400, "empty statement");
        }
        return sql;
    }

    private static String[] subPath(HttpExchange exchange, String prefix) throws HttpError {
        String path = exchange.getRequestURI().getPath();
        String rest = path.length() > prefix.length() ? path.substring(prefix.length()) : "";
        if (rest.startsWith("/")) {
            rest = rest.substring(1);
        }
        if (rest.isEmpty()) {
            throw new HttpError(404, "not found");
        }
        return rest.split("/");
    }

    private static long pathId(String[] segments) throws HttpError {
        try {
            return Long.parseLong(segments[0]);
        } catch (NumberFormatException e) {
            throw new HttpError(404, "not found");
        }
    }

    private static void sendResource(HttpExchange exchange, String resource,
            String contentType) {
        try (InputStream in = ConsoleServer.class.getResourceAsStream(resource)) {
            if (in == null) {
                send(exchange, 404, "application/json", "{\"error\":\"not found\"}");
                return;
            }
            send(exchange, 200, contentType,
                    new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            Log.error("console resource %s failed: %s", resource, e);
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
