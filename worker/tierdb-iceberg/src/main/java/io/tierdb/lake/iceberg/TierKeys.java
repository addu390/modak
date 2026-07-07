package io.tierdb.lake.iceberg;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.iceberg.types.Type;

/**
 * Conversions between the canonical i64 tier key and Iceberg's internal value
 * for the tier-key column (micros for timestamps, days for dates).
 */
public final class TierKeys {

    private TierKeys() {}

    public static Object internalValue(Type type, long canonical) {
        return switch (type.typeId()) {
            case LONG, TIMESTAMP -> canonical;
            case INTEGER, DATE -> Math.toIntExact(canonical);
            default -> throw new IllegalArgumentException(
                    "unsupported tier-key column type: " + type);
        };
    }

    public static long canonical(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof OffsetDateTime odt) {
            return Math.addExact(
                    Math.multiplyExact(odt.toEpochSecond(), 1_000_000L),
                    odt.getNano() / 1_000L);
        }
        if (value instanceof LocalDateTime ldt) {
            return canonical(ldt.atOffset(ZoneOffset.UTC));
        }
        if (value instanceof LocalDate d) {
            return d.toEpochDay();
        }
        throw new IllegalArgumentException("unsupported tier-key value: " + value
                + " (" + value.getClass().getName() + ")");
    }
}
