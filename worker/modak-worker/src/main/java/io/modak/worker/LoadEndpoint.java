package io.modak.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.modak.lake.LakeStorage;
import io.modak.load.Jsonl;
import io.modak.load.LoadClient;
import io.modak.load.LoadException;
import io.modak.load.LoadInFlightException;
import io.modak.load.LoadOptions;
import io.modak.load.LoadRequest;
import io.modak.load.LoadResult;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stream Load over HTTP: {@code POST /api/load/{schema}.{table}}, JSONL body,
 * {@code X-Modak-Label} header, JSON {@link LoadResult} back.
 */
public final class LoadEndpoint implements HttpHandler {

    public static final String PATH = "/api/load";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WorkerConfig config;
    private final Metrics metrics;
    private final Map<String, LakeStorage> lakes = new ConcurrentHashMap<>();

    private LoadEndpoint(WorkerConfig config, Metrics metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    /** Null (do not mount) when {@code MODAK_LOAD_TOKEN} is unset. */
    public static LoadEndpoint fromConfig(WorkerConfig config, Metrics metrics) {
        return config.loadToken() == null ? null : new LoadEndpoint(config, metrics);
    }

    @Override
    public void handle(HttpExchange exchange) {
        if (!"POST".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("POST only"));
            return;
        }
        if (!authorized(exchange)) {
            send(exchange, 401, error("missing or wrong load token"));
            return;
        }
        String table = tableOf(exchange.getRequestURI().getPath());
        if (table == null) {
            send(exchange, 400, error("path must be " + PATH + "/{schema}.{table}"));
            return;
        }
        String label = exchange.getRequestHeaders().getFirst("X-Modak-Label");
        if (label == null || label.isBlank()) {
            send(exchange, 400, error("X-Modak-Label header is required"));
            return;
        }
        try {
            List<Map<String, Object>> rows = Jsonl.parse(new BufferedReader(
                    new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8)));
            LoadResult result = client(table).load(new LoadRequest(label, rows));
            count(table, result);
            send(exchange, 200, result.toJson());
        } catch (LoadInFlightException e) {
            countState(table, "conflict");
            send(exchange, 409, error(e.getMessage()));
        } catch (LoadException e) {
            countState(table, "rejected");
            send(exchange, 400, error(e.getMessage()));
        } catch (Exception e) {
            Log.error("load '%s' into %s failed: %s", label, table, e);
            send(exchange, 500, error(String.valueOf(e)));
        }
    }

    private LoadClient client(String table) {
        return new LoadClient(LoadOptions.builder()
                .jdbcUrl(config.pgUrl())
                .credentials(config.pgUser(), config.pgPassword())
                .table(table)
                .spoolThreshold(config.loadSpoolThreshold())
                .build(), this::lakeFor);
    }

    private LakeStorage lakeFor(String format) {
        return lakes.computeIfAbsent(format, f -> LakePlugins.load(f, config.lakeConfig()));
    }

    private void count(String table, LoadResult result) {
        countState(table, result.replay() ? "replay" : result.state().sql());
        if (!result.replay()) {
            metrics.add(Metrics.series("modak_load_rows_total",
                    "table", table, "path", "heap"), result.hotRows());
            metrics.add(Metrics.series("modak_load_rows_total",
                    "table", table, "path", "delta"), result.deltaRows());
            metrics.add(Metrics.series("modak_load_rows_total",
                    "table", table, "path", "spool"), result.spooledRows());
        }
    }

    private void countState(String table, String state) {
        metrics.increment(Metrics.series("modak_load_total", "table", table, "state", state));
    }

    private boolean authorized(HttpExchange exchange) {
        String token = exchange.getRequestHeaders().getFirst("X-Modak-Token");
        if (token == null) {
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            if (auth != null && auth.startsWith("Bearer ")) {
                token = auth.substring("Bearer ".length());
            }
        }
        return config.loadToken().equals(token);
    }

    private static String tableOf(String path) {
        if (!path.startsWith(PATH + "/")) {
            return null;
        }
        String table = path.substring(PATH.length() + 1);
        return table.isBlank() || table.contains("/") ? null : table;
    }

    private static String error(String message) {
        return MAPPER.createObjectNode().put("error", message).toString();
    }

    private static void send(HttpExchange exchange, int status, String body) {
        try {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        } catch (Exception e) {
            Log.error("load response failed: %s", e);
        }
    }
}
