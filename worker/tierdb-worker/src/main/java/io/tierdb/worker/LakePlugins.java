package io.tierdb.worker;

import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.LakeStoragePlugin;
import java.util.Map;
import java.util.ServiceLoader;

/** {@link ServiceLoader} lookup of a {@link LakeStoragePlugin} by its {@code lake_format} id. */
final class LakePlugins {

    private LakePlugins() {}

    static LakeStorage load(String format, Map<String, String> config) {
        for (LakeStoragePlugin plugin : ServiceLoader.load(LakeStoragePlugin.class)) {
            if (plugin.identifier().equals(format)) {
                return plugin.create(config);
            }
        }
        throw new IllegalStateException("no LakeStoragePlugin for format: " + format);
    }
}
