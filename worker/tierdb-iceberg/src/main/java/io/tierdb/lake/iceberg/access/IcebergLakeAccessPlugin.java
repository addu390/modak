package io.tierdb.lake.iceberg.access;

import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeAccessPlugin;
import java.util.Map;

public final class IcebergLakeAccessPlugin implements LakeAccessPlugin {

    @Override
    public String identifier() {
        return "iceberg";
    }

    @Override
    public LakeAccess create(Map<String, String> config) {
        return new IcebergLakeAccess(config);
    }
}
