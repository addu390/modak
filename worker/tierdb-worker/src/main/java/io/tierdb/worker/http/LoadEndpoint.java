package io.tierdb.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.lake.LakeStorage;
import io.tierdb.load.Jsonl;
import io.tierdb.load.LoadClient;
import io.tierdb.load.LoadException;
import io.tierdb.load.LoadInFlightException;
import io.tierdb.load.LoadOptions;
import io.tierdb.load.LoadRequest;
import io.tierdb.load.LoadResult;
import io.tierdb.worker.LakeStorages;
import io.tierdb.worker.Log;
import io.tierdb.worker.WorkerConfig;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Stream Load over HTTP: {@code POST /api/load/{schema}.{table}}, JSONL body,
 * {@code X-TierDB-Label} header, JSON {@link LoadResult} back.
 */
public final class LoadEndpoint implements HttpHandler {

    public static final String PATH = "/api/load";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WorkerConfig config;
    private final Metrics metrics;
    private final JdbcCatalog catalog;
    private final LakeStorages lakes;

    private LoadEndpoint(WorkerConfig config, Metrics metrics) {
        this.config = config;
        this.metrics = metrics;
        this.catalog = new JdbcCatalog(config.dataSource());
        this.lakes = new LakeStorages(config, catalog);
    }

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
        String label = exchange.getRequestHeaders().getFirst("X-TierDB-Label");
        if (label == null || label.isBlank()) {
            send(exchange, 400, error("X-TierDB-Label header is required"));
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
                .build(), format -> lakeFor(table));
    }

    private LakeStorage lakeFor(String table) {
        String[] parts = table.split("\\.", 2);
        return catalog.lookup(parts[0], parts[1])
                .map(lakes::forTable)
                .orElseThrow(() -> new IllegalArgumentException(
                        table + " is not registered in tierdb.tables"));
    }

    private void count(String table, LoadResult result) {
        countState(table, result.replay() ? "replay" : result.state().sql());
        if (!result.replay()) {
            metrics.add(Metrics.series("tierdb_load_rows_total",
                    "table", table, "path", "heap"), result.hotRows());
            metrics.add(Metrics.series("tierdb_load_rows_total",
                    "table", table, "path", "delta"), result.deltaRows());
            metrics.add(Metrics.series("tierdb_load_rows_total",
                    "table", table, "path", "spool"), result.spooledRows());
        }
    }

    private void countState(String table, String state) {
        metrics.increment(Metrics.series("tierdb_load_total", "table", table, "state", state));
    }

    private boolean authorized(HttpExchange exchange) {
        String token = exchange.getRequestHeaders().getFirst("X-TierDB-Token");
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
