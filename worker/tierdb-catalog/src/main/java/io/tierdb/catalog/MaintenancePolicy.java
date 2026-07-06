package io.tierdb.catalog;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * A table's maintenance overrides, the {@code tierdb.tables.maintenance_policy}
 * jsonb. Keys and their meanings belong to the lake format, the catalog only
 * stores the overrides and layers them over the worker's defaults.
 */
public record MaintenancePolicy(Map<String, String> overrides) {

    public static final MaintenancePolicy NONE = new MaintenancePolicy(Map.of());

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public MaintenancePolicy {
        overrides = Collections.unmodifiableMap(new TreeMap<>(overrides));
    }

    public static MaintenancePolicy fromJson(String json) {
        if (json == null || json.isBlank()) {
            return NONE;
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            Map<String, String> overrides = new TreeMap<>();
            node.fields().forEachRemaining(e -> overrides.put(e.getKey(), e.getValue().asText()));
            return new MaintenancePolicy(overrides);
        } catch (Exception e) {
            throw new CatalogException("unreadable maintenance_policy: " + json, e);
        }
    }

    public String toJson() {
        if (overrides.isEmpty()) {
            return null;
        }
        ObjectNode node = MAPPER.createObjectNode();
        overrides.forEach((key, value) -> {
            if ("true".equals(value) || "false".equals(value)) {
                node.put(key, Boolean.parseBoolean(value));
            } else {
                try {
                    node.put(key, Long.parseLong(value));
                } catch (NumberFormatException notANumber) {
                    node.put(key, value);
                }
            }
        });
        return node.toString();
    }

    public Map<String, String> resolve(Map<String, String> defaults) {
        Map<String, String> settings = new TreeMap<>(defaults);
        settings.putAll(overrides);
        return settings;
    }
}
