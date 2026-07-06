package io.tierdb.worker.cli;

import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.MaintenancePolicy;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.worker.WorkerConfig;
import java.util.Map;
import java.util.TreeMap;

/**
 * The {@code tierdb-worker policy} command: view or edit a table's maintenance
 * overrides. Keys are format-interpreted, the command stores them verbatim.
 */
public final class PolicyCommand {

    private PolicyCommand() {}

    public static void run(WorkerConfig config, String[] args) {
        Args parsed = new Args(args);
        String qualified = parsed.required("--table");
        String[] parts = qualified.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("--table must be schema.table: " + qualified);
        }

        JdbcCatalog catalog = new JdbcCatalog(config.dataSource());
        RegisteredTable table = catalog.lookup(parts[0], parts[1])
                .orElseThrow(() -> new IllegalArgumentException(
                        "table is not registered: " + qualified));

        boolean reset = parsed.has("--reset");
        Map<String, String> sets = new TreeMap<>();
        for (String pair : parsed.all("--set")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException("--set expects key=value: '" + pair + "'");
            }
            sets.put(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
        }

        if (reset || !sets.isEmpty() || !parsed.all("--unset").isEmpty()) {
            Map<String, String> overrides = new TreeMap<>(
                    reset ? Map.of() : table.maintenancePolicy().overrides());
            parsed.all("--unset").forEach(overrides::remove);
            overrides.putAll(sets);
            catalog.setMaintenancePolicy(table.id(), new MaintenancePolicy(overrides));
            table = catalog.lookup(parts[0], parts[1]).orElseThrow();
            System.out.println("policy updated for " + qualified);
        }

        print(table, config.defaultMaintenanceSettings());
    }

    private static void print(RegisteredTable table, Map<String, String> defaults) {
        Map<String, String> overrides = table.maintenancePolicy().overrides();
        Map<String, String> settings = table.maintenancePolicy().resolve(defaults);
        System.out.println("maintenance settings for "
                + table.schemaName() + "." + table.tableName() + ":");
        settings.forEach((key, value) -> System.out.printf("  %-32s %-12s %s%n",
                key, value, overrides.containsKey(key) ? "(table)" : "(default)"));
    }
}
