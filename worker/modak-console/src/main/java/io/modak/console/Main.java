package io.modak.console;

import io.modak.worker.LoadEndpoint;
import io.modak.worker.WorkerConfig;
import io.modak.worker.WorkerDaemon;
import java.util.Map;

/**
 * Entrypoint for the console binary: a worker daemon plus the embedded web
 * console on one port. Every other command (register/unregister/verify)
 * delegates to the worker CLI, so this jar is a drop-in superset of it.
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) throws Exception {
        String command = args.length > 0 ? args[0] : "run";
        if (!command.equals("run")) {
            io.modak.worker.Main.main(args);
            return;
        }
        WorkerConfig config = WorkerConfig.fromEnv(System.getenv());
        int port = consolePort(System.getenv(), config);
        WorkerDaemon daemon = new WorkerDaemon(config.withMetricsPort(0));
        daemon.start();
        LoadEndpoint load = LoadEndpoint.fromConfig(config, daemon.metrics());
        ConsoleServer server = ConsoleServer.start(port, daemon.metrics(),
                new ConsoleData(config.dataSource()), daemon.seriesStore(),
                daemon::isLeading,
                config.consoleSql() ? new ConsoleQuery(config.consoleDataSource()) : null,
                load);
        Log.info("console on :%d (metrics at /metrics, sql %s, stream load %s)", server.port(),
                config.consoleSql() ? "enabled" : "disabled",
                load != null ? "enabled" : "disabled");
        Thread.currentThread().join();
    }

    static int consolePort(Map<String, String> env, WorkerConfig config) {
        String explicit = env.get("MODAK_CONSOLE_PORT");
        if (explicit != null && !explicit.isBlank()) {
            return Integer.parseInt(explicit);
        }
        return config.metricsPort() > 0 ? config.metricsPort() : 9090;
    }
}
