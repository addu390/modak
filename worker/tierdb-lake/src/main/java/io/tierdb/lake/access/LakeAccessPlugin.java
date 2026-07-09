package io.tierdb.lake.access;

import java.util.Map;
import java.util.ServiceLoader;

/** SPI for pluggable lake formats (discovered via {@link ServiceLoader}). */
public interface LakeAccessPlugin {

    String identifier();

    LakeAccess create(Map<String, String> config);

    static LakeAccess load(String format, Map<String, String> config) {
        // Resolve against the classloader owning the SPI so isolated plugin
        // classloaders (Trino) find adapters shipped alongside this jar.
        for (LakeAccessPlugin plugin : ServiceLoader.load(
                LakeAccessPlugin.class, LakeAccessPlugin.class.getClassLoader())) {
            if (plugin.identifier().equals(format)) {
                return plugin.create(config);
            }
        }
        throw new IllegalStateException("no LakeAccessPlugin for format: " + format);
    }
}
