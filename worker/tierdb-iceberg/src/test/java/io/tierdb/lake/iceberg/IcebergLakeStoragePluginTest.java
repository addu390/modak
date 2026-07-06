package io.tierdb.lake.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import io.tierdb.lake.LakeStoragePlugin;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import io.tierdb.lake.LakePartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergLakeStoragePluginTest {

    @TempDir
    Path warehouse;

    @Test
    void icebergPluginIsDiscoverableAndKeyedByIdentifier() {
        var plugin = ServiceLoader.load(LakeStoragePlugin.class).stream()
                .map(ServiceLoader.Provider::get)
                .filter(p -> IcebergLakeStoragePlugin.IDENTIFIER.equals(p.identifier()))
                .findFirst();

        assertTrue(plugin.isPresent(),
                "iceberg LakeStoragePlugin must be registered via META-INF/services");
        assertNotNull(plugin.get().create(Map.of()),
                "plugin.create() should return a LakeStorage");
    }

    @Test
    void tableRefIsTheWarehousePathWhenNoCatalogIsConfigured() {
        var pathBased = new IcebergLakeStorage(Map.of("warehouse", warehouse + "/"));
        assertEquals(warehouse + "/public.events", pathBased.tableRef("public", "events"));
    }

    @Test
    void icebergTablePropsPassThroughToTheCreatedTable() {
        var storage = new IcebergLakeStorage(Map.of(
                "warehouse", warehouse.toString(),
                "iceberg.table.write.parquet.compression-codec", "gzip",
                "iceberg.catalog.ignored-here", "catalog props only matter with a catalog"));
        String ref = storage.tableRef("public", "events");
        String metadataLocation = storage.createTableIfAbsent(ref,
                List.of(new Column("id", ColumnType.LONG), new Column("t", ColumnType.LONG)),
                Set.of("id", "t"), "t", LakePartition.none());

        assertNotNull(metadataLocation);
        var table = storage.tables().load(ref);
        assertEquals("gzip", table.properties().get("write.parquet.compression-codec"));
    }
}
