package io.tierdb.lake.iceberg;

import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.LakeStoragePlugin;
import java.util.Map;

/** SPI entry point for the Iceberg cold-store format (registered via ServiceLoader). */
public final class IcebergLakeStoragePlugin implements LakeStoragePlugin {

    public static final String IDENTIFIER = "iceberg";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public LakeStorage create(Map<String, String> config) {
        return new IcebergLakeStorage(config);
    }
}
