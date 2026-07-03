package io.modak.lake.iceberg;

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;

/**
 * The one partition layout Modak creates: unpartitioned, or a single
 * {@code truncate(tier_key, width)} field. Owns all bucket math so no caller
 * parses the spec itself.
 */
final class TruncatePartitioning {

    private final PartitionSpec spec;
    private final Schema schema;
    private final String sourceColumn;
    private final long width;

    private TruncatePartitioning(PartitionSpec spec, Schema schema,
            String sourceColumn, long width) {
        this.spec = spec;
        this.schema = schema;
        this.sourceColumn = sourceColumn;
        this.width = width;
    }

    static TruncatePartitioning of(Table table) {
        PartitionSpec spec = table.spec();
        if (spec.isUnpartitioned()) {
            return new TruncatePartitioning(spec, table.schema(), null, 0);
        }
        if (spec.fields().size() != 1) {
            throw new IllegalArgumentException(
                    "expected a single truncate(tier_key) partition spec, found " + spec);
        }
        String transform = spec.fields().get(0).transform().toString();
        if (!transform.startsWith("truncate[")) {
            throw new IllegalArgumentException(
                    "expected a single truncate(tier_key) partition spec, found " + spec);
        }
        long width = Long.parseLong(
                transform.substring("truncate[".length(), transform.length() - 1));
        String sourceColumn = table.schema().findColumnName(spec.fields().get(0).sourceId());
        return new TruncatePartitioning(spec, table.schema(), sourceColumn, width);
    }

    boolean partitioned() {
        return sourceColumn != null;
    }

    String sourceColumn() {
        return sourceColumn;
    }

    /**
     * The partition key holding the given tier key, or {@code null} when the
     * table is unpartitioned (Iceberg's own convention for writer APIs).
     */
    PartitionKey keyOf(long tierKey) {
        if (!partitioned()) {
            return null;
        }
        GenericRecord probe = GenericRecord.create(schema);
        probe.setField(sourceColumn, tierKey);
        PartitionKey key = new PartitionKey(spec, schema);
        key.partition(probe);
        return key;
    }

    /**
     * The partition path for a file spanning {@code [lo, hi]}, which must lie
     * in one bucket. A straddling file is rejected with the offending path named.
     */
    String singleBucketPath(long lo, long hi, String path) {
        long bucket = Math.floorDiv(lo, width) * width;
        if (Math.floorDiv(hi, width) * width != bucket) {
            throw new IllegalArgumentException(path + " straddles the partition boundary at "
                    + (bucket + width) + "; split the file at multiples of " + width);
        }
        return spec.fields().get(0).name() + "=" + bucket;
    }
}
