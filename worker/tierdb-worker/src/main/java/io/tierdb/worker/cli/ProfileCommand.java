package io.tierdb.worker.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.StorageProfile;
import io.tierdb.worker.Log;
import io.tierdb.worker.WorkerConfig;
import java.util.Map;

/** The {@code tierdb-worker profile} subcommand: list and create storage profiles. */
public final class ProfileCommand {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ProfileCommand() {}

    public static void run(WorkerConfig config, String[] args) throws Exception {
        String action = args.length > 1 ? args[1] : "list";
        JdbcCatalog catalog = new JdbcCatalog(config.dataSource());
        switch (action) {
            case "list" -> list(catalog);
            case "create" -> create(catalog, new Args(args));
            default -> throw new IllegalArgumentException(
                    "unknown profile action '" + action + "' (list, create)");
        }
    }

    private static void list(JdbcCatalog catalog) {
        for (StorageProfile p : catalog.listStorageProfiles()) {
            System.out.printf("%-20s %-10s %-40s %-16s %s%n",
                    p.name(),
                    p.lakeFormat() == null ? "(env)" : p.lakeFormat(),
                    p.warehouse().isBlank() ? "(env)" : p.warehouse(),
                    p.credentialRef() == null ? "(default)" : p.credentialRef(),
                    p.isDefault() ? "default" : "");
        }
    }

    private static void create(JdbcCatalog catalog, Args parsed) {
        String name = parsed.required("--name");
        String warehouse = parsed.required("--warehouse");
        String format = parsed.optional("--format", null);
        String credentials = parsed.optional("--credentials", null);
        String configJson = configJson(parsed.optional("--config", null));
        boolean isDefault = parsed.has("--default");
        catalog.createStorageProfile(new StorageProfile(
                name, format, warehouse, configJson, credentials, isDefault));
        Log.info("created storage profile '%s' (warehouse %s)", name, warehouse);
    }

    private static String configJson(String pairs) {
        if (pairs == null || pairs.isBlank()) {
            return null;
        }
        ObjectNode node = MAPPER.createObjectNode();
        for (Map.Entry<String, String> e : WorkerConfig.parseLakeProps(pairs).entrySet()) {
            node.put(e.getKey(), e.getValue());
        }
        return node.toString();
    }
}
