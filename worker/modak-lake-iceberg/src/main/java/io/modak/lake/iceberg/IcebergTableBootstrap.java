package io.modak.lake.iceberg;

import io.modak.common.RowBatchData.Column;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Creates the cold Iceberg table for a newly registered hot table (idempotent).
 * A positive partition width lays it out as {@code truncate(tier_key, width)},
 * zero means unpartitioned. The spec applies at creation only.
 */
public final class IcebergTableBootstrap {

    private IcebergTableBootstrap() {}

    /** Creates the table when absent, returns its current metadata file location. */
    public static String createIfAbsent(IcebergTables tables, String ref,
            List<Column> columns, Set<String> requiredCols,
            String tierKeyCol, long partitionWidth) {
        Types.NestedField[] fields = new Types.NestedField[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Type type = IcebergSchemaEvolution.icebergType(col);
            fields[i] = requiredCols.contains(col.name())
                    ? Types.NestedField.required(i + 1, col.name(), type)
                    : Types.NestedField.optional(i + 1, col.name(), type);
        }
        Schema schema = new Schema(fields);
        if (partitionWidth > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("partition width " + partitionWidth
                    + " exceeds Iceberg's truncate limit (" + Integer.MAX_VALUE
                    + "); pass an explicit --partition-width or register unpartitioned");
        }
        PartitionSpec spec = partitionWidth > 0
                ? PartitionSpec.builderFor(schema).truncate(tierKeyCol, (int) partitionWidth).build()
                : PartitionSpec.unpartitioned();
        Table table = tables.createIfAbsent(ref, schema, spec);
        return ((BaseTable) table).operations().current().metadataFileLocation();
    }
}
