package io.tierdb.lake.iceberg;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.LakePartition;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Creates the cold Iceberg table for a newly registered hot table (idempotent).
 * The requested partition layout applies at creation only, truncate for
 * numeric tier keys and hour/day/month/year for temporal ones.
 */
public final class IcebergTableBootstrap {

    private IcebergTableBootstrap() {}

    public static Map<String, String> createIfAbsent(IcebergTables tables, String ref,
            List<Column> columns, Set<String> requiredCols,
            String tierKeyCol, LakePartition partition) {
        Types.NestedField[] fields = new Types.NestedField[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Type type = IcebergSchemaEvolution.icebergType(col);
            fields[i] = requiredCols.contains(col.name())
                    ? Types.NestedField.required(i + 1, col.name(), type)
                    : Types.NestedField.optional(i + 1, col.name(), type);
        }
        Schema schema = new Schema(fields);
        Table table = tables.createIfAbsent(ref, schema, spec(schema, tierKeyCol, partition));
        return IcebergPublish.props(table);
    }

    private static PartitionSpec spec(Schema schema, String tierKeyCol, LakePartition partition) {
        if (partition.isNone()) {
            return PartitionSpec.unpartitioned();
        }
        if (partition.isTruncate()) {
            long width = partition.truncateWidth();
            if (width > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("partition width " + width
                        + " exceeds Iceberg's truncate limit (" + Integer.MAX_VALUE
                        + "), pass an explicit --partition-width or register unpartitioned");
            }
            return PartitionSpec.builderFor(schema).truncate(tierKeyCol, (int) width).build();
        }
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        switch (partition.transform()) {
            case "hour" -> builder.hour(tierKeyCol);
            case "day" -> builder.day(tierKeyCol);
            case "month" -> builder.month(tierKeyCol);
            case "year" -> builder.year(tierKeyCol);
            default -> throw new IllegalArgumentException(
                    "unsupported lake partition transform: " + partition.transform());
        }
        return builder.build();
    }
}
