package io.modak.lake.iceberg;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionKey;

/**
 * One writer per partition, created on first use. The unpartitioned case is a
 * single writer under Iceberg's null-partition convention, confined here so no
 * algorithm handles it inline.
 */
final class PartitionFanout<W> {

    interface Factory<W> {
        W create(PartitionKey partition) throws IOException;
    }

    private final TierKeyPartitioning partitioning;
    private final Factory<W> factory;
    private final Map<PartitionKey, W> writers = new HashMap<>();

    PartitionFanout(TierKeyPartitioning partitioning, Factory<W> factory) {
        this.partitioning = partitioning;
        this.factory = factory;
    }

    W writerFor(long tierKey) throws IOException {
        PartitionKey key = partitioning.keyOf(tierKey);
        W writer = writers.get(key);
        if (writer == null) {
            writer = factory.create(key);
            writers.put(key, writer);
        }
        return writer;
    }

    Collection<W> all() {
        return writers.values();
    }
}
