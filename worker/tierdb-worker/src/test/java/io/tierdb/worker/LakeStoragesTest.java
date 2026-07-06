package io.tierdb.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.InMemoryCatalog;
import io.tierdb.catalog.StorageProfile;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LakeStoragesTest {

    private final InMemoryCatalog catalog = new InMemoryCatalog();

    private LakeStorages storages(WorkerConfig config, Map<String, String> env) {
        return new LakeStorages(config, catalog, env);
    }

    @Test
    void defaultProfileIsTheWorkerEnvConfig() {
        WorkerConfig config = WorkerConfig.builder()
                .warehouse("/tmp/wh")
                .lakeConfig(Map.of("s3.endpoint", "http://minio:9000"))
                .build();
        LakeStorages storages = storages(config, Map.of());

        Map<String, String> resolved =
                storages.lakeConfigFor(catalog.defaultStorageProfile());

        assertEquals(config.lakeConfig(), resolved);
        assertEquals("iceberg", storages.formatOf(catalog.defaultStorageProfile()));
    }

    @Test
    void profileOverridesWarehouseAndMergesAnyProviderKeys() {
        WorkerConfig config = WorkerConfig.builder().warehouse("/tmp/wh").build();
        catalog.createStorageProfile(new StorageProfile("gcs-eu", "delta",
                "gs://analytics-eu",
                "{\"gcs.project-id\": \"acme-eu\"}", null, false));
        LakeStorages storages = storages(config, Map.of());

        StorageProfile profile = storages.profile("gcs-eu");
        Map<String, String> resolved = storages.lakeConfigFor(profile);

        assertEquals("gs://analytics-eu", resolved.get("warehouse"));
        assertEquals("acme-eu", resolved.get("gcs.project-id"));
        assertEquals("delta", storages.formatOf(profile));
    }

    @Test
    void credentialRefMergesAnEnvFragmentWithoutInterpretingIt() {
        WorkerConfig config = WorkerConfig.builder()
                .lakeConfig(Map.of("s3.access-key", "worker-default"))
                .build();
        catalog.createStorageProfile(new StorageProfile("analytics", null,
                "s3://analytics", null, "analytics", false));
        LakeStorages storages = storages(config, Map.of(
                "TIERDB_CREDENTIALS_ANALYTICS",
                "s3.access-key=team-key;s3.secret-key=team-secret"));

        Map<String, String> resolved =
                storages.lakeConfigFor(storages.profile("analytics"));

        assertEquals("team-key", resolved.get("s3.access-key"));
        assertEquals("team-secret", resolved.get("s3.secret-key"));
    }

    @Test
    void blankValueDropsAnInheritedKeyToReachTheAmbientChain() {
        WorkerConfig config = WorkerConfig.builder()
                .lakeConfig(Map.of("s3.access-key", "worker-default",
                        "s3.secret-key", "worker-secret"))
                .build();
        catalog.createStorageProfile(new StorageProfile("iam-role", null,
                "s3://prod", "{\"s3.access-key\": \"\", \"s3.secret-key\": \"\"}",
                null, false));
        LakeStorages storages = storages(config, Map.of());

        Map<String, String> resolved =
                storages.lakeConfigFor(storages.profile("iam-role"));

        assertFalse(resolved.containsKey("s3.access-key"));
        assertFalse(resolved.containsKey("s3.secret-key"));
    }

    @Test
    void unresolvableCredentialRefFailsWithTheEnvVarName() {
        WorkerConfig config = WorkerConfig.builder().build();
        catalog.createStorageProfile(new StorageProfile("missing", null,
                "s3://x", null, "no-such-set", false));
        LakeStorages storages = storages(config, Map.of());

        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> storages.lakeConfigFor(storages.profile("missing")));
        assertTrue(e.getMessage().contains("TIERDB_CREDENTIALS_NO_SUCH_SET"));
    }

    @Test
    void unknownProfileFails() {
        LakeStorages storages = storages(WorkerConfig.builder().build(), Map.of());
        assertThrows(IllegalArgumentException.class, () -> storages.profile("ghost"));
    }
}
