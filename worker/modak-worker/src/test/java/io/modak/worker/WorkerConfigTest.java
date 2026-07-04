package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class WorkerConfigTest {

    @Test
    void lakePropsRideVerbatimIntoTheLakeConfig() {
        WorkerConfig config = WorkerConfig.fromEnv(Map.of(
                "MODAK_WAREHOUSE", "/wh",
                "MODAK_LAKE_PROPS",
                "iceberg.table.write.parquet.compression-codec=gzip;"
                        + " iceberg.catalog.oauth2-server-uri=https://idp/token ;"));

        assertEquals("gzip",
                config.lakeConfig().get("iceberg.table.write.parquet.compression-codec"));
        assertEquals("https://idp/token",
                config.lakeConfig().get("iceberg.catalog.oauth2-server-uri"));
    }

    @Test
    void malformedLakePropsFailLoudly() {
        assertThrows(IllegalArgumentException.class,
                () -> WorkerConfig.fromEnv(Map.of("MODAK_LAKE_PROPS", "no-equals-sign")));
    }

    @Test
    void warehouseAndFormatArepartOfTheLakeConfig() {
        WorkerConfig config = WorkerConfig.fromEnv(Map.of("MODAK_WAREHOUSE", "/wh"));
        assertEquals("/wh", config.lakeConfig().get("warehouse"));
        assertEquals("iceberg", config.lakeFormat());

        WorkerConfig other = WorkerConfig.fromEnv(Map.of("MODAK_LAKE_FORMAT", "delta"));
        assertEquals("delta", other.lakeFormat());
    }

    @Test
    void manualConstructionCarriesTheWarehouseToo() {
        WorkerConfig config = WorkerConfig.builder()
                .pgUrl("jdbc:x").pgUser("u").warehouse("/wh").build();
        assertTrue(config.lakeConfig().containsKey("warehouse"));
        assertEquals("/wh", config.lakeConfig().get("warehouse"));
    }
}
