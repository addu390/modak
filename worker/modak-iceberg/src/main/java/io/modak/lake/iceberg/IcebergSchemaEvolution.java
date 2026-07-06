package io.modak.lake.iceberg;

import io.modak.common.RowBatchData.Column;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Additive schema evolution for both write paths (tiered flush and mirror pump):
 * adds any columns missing from the table's schema as optional fields, in one
 * schema commit. Idempotent, so safe to replay across crashes.
 */
final class IcebergSchemaEvolution {

    private final Table table;

    IcebergSchemaEvolution(Table table) {
        this.table = table;
    }

    boolean addMissing(List<Column> columns) {
        UpdateSchema update = table.updateSchema();
        boolean any = false;
        for (Column col : columns) {
            if (table.schema().findField(col.name()) != null) {
                continue;
            }
            update.addColumn(col.name(), icebergType(col));
            any = true;
        }
        if (any) {
            update.commit();
            table.refresh();
        }
        return any;
    }

    static Type icebergType(Column col) {
        return switch (col.type()) {
            case LONG -> Types.LongType.get();
            case DOUBLE -> Types.DoubleType.get();
            case BOOLEAN -> Types.BooleanType.get();
            case TEXT -> Types.StringType.get();
            case TIMESTAMP -> Types.TimestampType.withZone();
            case DATE -> Types.DateType.get();
            case DECIMAL -> Types.DecimalType.of(col.precision(), col.scale());
            case UUID -> Types.UUIDType.get();
            case BINARY -> Types.BinaryType.get();
        };
    }
}
