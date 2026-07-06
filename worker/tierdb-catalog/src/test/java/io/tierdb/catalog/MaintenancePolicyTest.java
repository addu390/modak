package io.tierdb.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class MaintenancePolicyTest {

    @Test
    void nullAndBlankJsonMeanNoOverrides() {
        assertEquals(MaintenancePolicy.NONE, MaintenancePolicy.fromJson(null));
        assertEquals(MaintenancePolicy.NONE, MaintenancePolicy.fromJson("  "));
        assertEquals(MaintenancePolicy.NONE, MaintenancePolicy.fromJson("{}"));
    }

    @Test
    void jsonScalarsRoundTripThroughStrings() {
        MaintenancePolicy policy = MaintenancePolicy.fromJson(
                "{\"snapshot_retention_hours\": 6, \"orphan_sweep_enabled\": true, "
                        + "\"note\": \"weekly\"}");
        assertEquals("6", policy.overrides().get("snapshot_retention_hours"));
        assertEquals("true", policy.overrides().get("orphan_sweep_enabled"));

        assertEquals(policy, MaintenancePolicy.fromJson(policy.toJson()));
        assertEquals("{\"note\":\"weekly\",\"orphan_sweep_enabled\":true,"
                + "\"snapshot_retention_hours\":6}", policy.toJson());
    }

    @Test
    void emptyPolicySerializesToNullSoTheColumnClears() {
        assertNull(MaintenancePolicy.NONE.toJson());
    }

    @Test
    void resolveLayersOverridesOverDefaults() {
        MaintenancePolicy policy = new MaintenancePolicy(
                Map.of("snapshot_retention_hours", "6"));
        Map<String, String> settings = policy.resolve(Map.of(
                "snapshot_retention_hours", "24", "snapshot_min_retained", "5"));
        assertEquals("6", settings.get("snapshot_retention_hours"));
        assertEquals("5", settings.get("snapshot_min_retained"));
    }

    @Test
    void malformedJsonIsRejected() {
        assertThrows(CatalogException.class, () -> MaintenancePolicy.fromJson("not json"));
    }
}
