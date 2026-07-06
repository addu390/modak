package io.tierdb.catalog;

import java.util.Objects;

/**
 * A row of {@code tierdb.storage_profiles}: a named warehouse binding.
 * {@code credentialRef} names a credential set in the worker's environment,
 * keys never live here. Blank warehouse / null format mean "worker env".
 */
public record StorageProfile(
        String name,
        String lakeFormat,
        String warehouse,
        String lakeConfigJson,
        String credentialRef,
        boolean isDefault) {

    public StorageProfile {
        Objects.requireNonNull(name);
        Objects.requireNonNull(warehouse);
        if (name.isBlank()) {
            throw new IllegalArgumentException("profile name must be non-blank");
        }
    }
}
