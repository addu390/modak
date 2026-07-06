package io.tierdb.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tierdb.catalog.Catalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.StorageProfile;
import io.tierdb.lake.LakeStorage;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Resolves {@link LakeStorage} instances through storage profiles, layering profile overrides and named credentials over the worker's defaults. */
public final class LakeStorages {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final WorkerConfig config;
    private final Catalog catalog;
    private final Map<String, String> env;
    private final Map<String, LakeStorage> cache = new ConcurrentHashMap<>();

    public LakeStorages(WorkerConfig config, Catalog catalog) {
        this(config, catalog, System.getenv());
    }

    LakeStorages(WorkerConfig config, Catalog catalog, Map<String, String> env) {
        this.config = config;
        this.catalog = catalog;
        this.env = env;
    }

    public LakeStorage forTable(RegisteredTable table) {
        return forProfile(profile(table.storageProfile()), table.lakeFormat());
    }

    public LakeStorage forProfile(StorageProfile profile) {
        return forProfile(profile, formatOf(profile));
    }

    private LakeStorage forProfile(StorageProfile profile, String format) {
        return cache.computeIfAbsent(profile.name() + "|" + format,
                key -> LakePlugins.load(format, lakeConfigFor(profile)));
    }

    public StorageProfile profile(String name) {
        return catalog.storageProfile(name).orElseThrow(() -> new IllegalArgumentException(
                "unknown storage profile '" + name + "' (tierdb.storage_profiles)"));
    }

    public String formatOf(StorageProfile profile) {
        return profile.lakeFormat() == null || profile.lakeFormat().isBlank()
                ? config.lakeFormat()
                : profile.lakeFormat();
    }

    public void clear() {
        cache.clear();
    }

    Map<String, String> lakeConfigFor(StorageProfile profile) {
        Map<String, String> merged = new HashMap<>(config.lakeConfig());
        if (!profile.warehouse().isBlank()) {
            merged.put("warehouse", profile.warehouse());
        }
        overlay(merged, parseConfig(profile.lakeConfigJson()));
        if (profile.credentialRef() != null && !profile.credentialRef().isBlank()) {
            overlay(merged, credentials(profile.credentialRef()));
        }
        return merged;
    }

    private Map<String, String> credentials(String ref) {
        String var = "TIERDB_CREDENTIALS_"
                + ref.toUpperCase(Locale.ROOT).replace('-', '_');
        String raw = env.get(var);
        if (raw == null) {
            throw new IllegalStateException("credential_ref '" + ref + "' names no "
                    + "credential set in this worker's environment (set " + var
                    + " as key=value pairs, ';'-separated)");
        }
        return WorkerConfig.parseLakeProps(raw);
    }

    private static void overlay(Map<String, String> base, Map<String, String> patch) {
        patch.forEach((key, value) -> {
            if (value == null || value.isBlank()) {
                base.remove(key);
            } else {
                base.put(key, value);
            }
        });
    }

    private static Map<String, String> parseConfig(String json) {
        Map<String, String> out = new HashMap<>();
        if (json == null || json.isBlank()) {
            return out;
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> e = fields.next();
                out.put(e.getKey(), e.getValue().asText());
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("storage profile lake_config is not a "
                    + "flat JSON object: " + json, e);
        }
        return out;
    }
}
