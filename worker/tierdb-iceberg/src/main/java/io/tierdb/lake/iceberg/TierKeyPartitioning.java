package io.tierdb.lake.iceberg;

import java.util.Set;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;

/**
 * The partition layouts TierDB creates: unpartitioned, truncate(tier_key, w),
 * or a temporal transform on the tier key. Owns the bucket math so no caller
 * parses the spec itself.
 */
final class TierKeyPartitioning {

    private static final Set<String> TEMPORAL = Set.of("hour", "day", "month", "year");

    private final PartitionSpec spec;
    private final Schema schema;
    private final String sourceColumn;
    private final long truncateWidth;

    private TierKeyPartitioning(PartitionSpec spec, Schema schema,
            String sourceColumn, long truncateWidth) {
        this.spec = spec;
        this.schema = schema;
        this.sourceColumn = sourceColumn;
        this.truncateWidth = truncateWidth;
    }

    static TierKeyPartitioning of(Table table) {
        PartitionSpec spec = table.spec();
        if (spec.isUnpartitioned()) {
            return new TierKeyPartitioning(spec, table.schema(), null, 0);
        }
        if (spec.fields().size() != 1) {
            throw new IllegalArgumentException(
                    "expected a single tier-key partition field, found " + spec);
        }
        String transform = spec.fields().get(0).transform().toString();
        String sourceColumn = table.schema().findColumnName(spec.fields().get(0).sourceId());
        if (transform.startsWith("truncate[")) {
            long width = Long.parseLong(
                    transform.substring("truncate[".length(), transform.length() - 1));
            return new TierKeyPartitioning(spec, table.schema(), sourceColumn, width);
        }
        if (TEMPORAL.contains(transform)) {
            return new TierKeyPartitioning(spec, table.schema(), sourceColumn, 0);
        }
        throw new IllegalArgumentException(
                "expected a truncate or temporal tier-key partition spec, found " + spec);
    }

    boolean partitioned() {
        return sourceColumn != null;
    }

    String sourceColumn() {
        return sourceColumn;
    }

    PartitionKey keyOf(long tierKey) {
        if (!partitioned()) {
            return null;
        }
        GenericRecord probe = GenericRecord.create(schema);
        probe.setField(sourceColumn, TierKeys.internalValue(
                schema.findField(sourceColumn).type(), tierKey));
        PartitionKey key = new PartitionKey(spec, schema);
        key.partition(probe);
        return key;
    }

    String singleBucketPath(long lo, long hi, String path) {
        if (truncateWidth <= 0) {
            throw new IllegalArgumentException("bulk file adoption needs a truncate layout, "
                    + spec + " partitions by a temporal transform; stage rows instead");
        }
        long bucket = Math.floorDiv(lo, truncateWidth) * truncateWidth;
        if (Math.floorDiv(hi, truncateWidth) * truncateWidth != bucket) {
            throw new IllegalArgumentException(path + " straddles the partition boundary at "
                    + (bucket + truncateWidth) + "; split the file at multiples of "
                    + truncateWidth);
        }
        return spec.fields().get(0).name() + "=" + bucket;
    }
}
