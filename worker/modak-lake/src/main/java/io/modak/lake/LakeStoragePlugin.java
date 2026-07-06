package io.modak.lake;

import java.util.Map;

/** SPI for pluggable lake formats (discovered via {@link java.util.ServiceLoader}). */
public interface LakeStoragePlugin {
    String identifier();

    LakeStorage create(Map<String, String> config);
}
