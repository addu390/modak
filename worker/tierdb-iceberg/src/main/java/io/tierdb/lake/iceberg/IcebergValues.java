package io.tierdb.lake.iceberg;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Coercions from TierDB's neutral Java values to Iceberg record values. */
public final class IcebergValues {

    private IcebergValues() {}

    public static Object coerce(Object v, Type type) {
        if (v == null) {
            return null;
        }
        if (type.typeId() == Type.TypeID.INTEGER && v instanceof Long l) {
            return Math.toIntExact(l);
        }
        if (type.typeId() == Type.TypeID.FLOAT && v instanceof Double d) {
            return (float) (double) d;
        }
        if (type instanceof Types.DecimalType dec && v instanceof BigDecimal bd) {
            return bd.setScale(dec.scale(), RoundingMode.HALF_UP);
        }
        if (type.typeId() == Type.TypeID.BINARY && v instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes);
        }
        if (type instanceof Types.TimestampType ts && v instanceof OffsetDateTime odt
                && !ts.shouldAdjustToUTC()) {
            return odt.toLocalDateTime();
        }
        return v;
    }
}
