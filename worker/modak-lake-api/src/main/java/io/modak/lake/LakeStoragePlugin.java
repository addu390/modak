package io.modak.lake;

import java.util.Map;

/**
 * SPI for pluggable lake formats (discovered via {@link java.util.ServiceLoader}).
 * The {@link #identifier()} equals the {@code lake_format} discriminator stored
 * in {@code modak.tables}. This is the OCP seam, a new format is a new plugin,
 * not a change to the core catalog or the workers.
 */
public interface LakeStoragePlugin {
    String identifier();

    LakeStorage create(Map<String, String> config);
}
