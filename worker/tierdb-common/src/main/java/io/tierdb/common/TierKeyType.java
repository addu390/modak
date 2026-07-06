package io.tierdb.common;

import io.tierdb.common.RowBatchData.ColumnType;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * The tier-key column's native type and its order-preserving codec onto the
 * canonical i64 axis. The catalog and the protocol stay canonical, the codec
 * applies only at the boundaries.
 */
public enum TierKeyType {
    BIGINT("bigint", ColumnType.LONG),
    TIMESTAMPTZ("timestamptz", ColumnType.TIMESTAMP),
    TIMESTAMP("timestamp", ColumnType.TIMESTAMP),
    DATE("date", ColumnType.DATE);

    private static final DateTimeFormatter TS_LITERAL =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TS_TOKEN =
            DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmss");

    private final String sql;
    private final ColumnType columnType;

    TierKeyType(String sql, ColumnType columnType) {
        this.sql = sql;
        this.columnType = columnType;
    }

    public String sql() {
        return sql;
    }

    public ColumnType columnType() {
        return columnType;
    }

    public static TierKeyType forType(String pgTypeName) {
        return switch (pgTypeName.toLowerCase(Locale.ROOT)) {
            case "int2", "smallint", "int4", "integer", "int8", "bigint" -> BIGINT;
            case "timestamptz", "timestamp with time zone" -> TIMESTAMPTZ;
            case "timestamp", "timestamp without time zone" -> TIMESTAMP;
            case "date" -> DATE;
            default -> throw new IllegalArgumentException(
                    "unsupported tier key type: " + pgTypeName
                    + " (supported: bigint, integer, smallint, timestamptz, timestamp, date)");
        };
    }

    public long encode(Object value) {
        return switch (this) {
            case BIGINT -> encodeLong(value);
            case TIMESTAMPTZ, TIMESTAMP -> encodeMicros(value);
            case DATE -> encodeDays(value);
        };
    }

    public Object decode(long canonical) {
        return switch (this) {
            case BIGINT -> canonical;
            case TIMESTAMPTZ, TIMESTAMP -> OffsetDateTime.ofInstant(
                    java.time.Instant.EPOCH.plus(canonical, java.time.temporal.ChronoUnit.MICROS),
                    ZoneOffset.UTC);
            case DATE -> LocalDate.ofEpochDay(canonical);
        };
    }

    public String pgLiteral(long canonical) {
        return switch (this) {
            case BIGINT -> Long.toString(canonical);
            case TIMESTAMPTZ -> "TIMESTAMPTZ '" + TS_LITERAL.format((OffsetDateTime) decode(canonical)) + "+00'";
            case TIMESTAMP -> "TIMESTAMP '" + TS_LITERAL.format((OffsetDateTime) decode(canonical)) + "'";
            case DATE -> "DATE '" + decode(canonical) + "'";
        };
    }

    public long parseBoundLiteral(String text) {
        return switch (this) {
            case BIGINT -> Long.parseLong(unquote(text));
            case TIMESTAMPTZ, TIMESTAMP, DATE -> encode(unquote(text));
        };
    }

    public String nameToken(long canonical) {
        return switch (this) {
            case BIGINT -> canonical < 0 ? "m" + Math.abs(canonical) : Long.toString(canonical);
            case TIMESTAMPTZ, TIMESTAMP -> TS_TOKEN.format((OffsetDateTime) decode(canonical));
            case DATE -> DateTimeFormatter.BASIC_ISO_DATE.format((LocalDate) decode(canonical));
        };
    }

    public long parseLagOrWidth(String text) {
        return switch (this) {
            case BIGINT -> Long.parseLong(text.trim());
            case TIMESTAMPTZ, TIMESTAMP -> parseDuration(text, 1_000_000L, 86_400_000_000L);
            case DATE -> parseDuration(text, -1, 1);
        };
    }

    private static long encodeLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        if (value instanceof String s) {
            return Long.parseLong(s.trim());
        }
        throw new IllegalArgumentException(
                "tier key value is not a number: " + value + " (" + value.getClass().getName() + ")");
    }

    private static long encodeMicros(Object value) {
        OffsetDateTime odt;
        if (value instanceof OffsetDateTime v) {
            odt = v;
        } else if (value instanceof LocalDateTime v) {
            odt = v.atOffset(ZoneOffset.UTC);
        } else if (value instanceof java.sql.Timestamp v) {
            odt = v.toInstant().atOffset(ZoneOffset.UTC);
        } else if (value instanceof java.time.Instant v) {
            odt = v.atOffset(ZoneOffset.UTC);
        } else if (value instanceof String s) {
            odt = PgValues.parseTimestamp(s.trim());
        } else {
            throw new IllegalArgumentException(
                    "tier key value is not a timestamp: " + value
                    + " (" + value.getClass().getName() + ")");
        }
        return Math.addExact(
                Math.multiplyExact(odt.toEpochSecond(), 1_000_000L),
                odt.getNano() / 1_000L);
    }

    private static long encodeDays(Object value) {
        LocalDate d;
        if (value instanceof LocalDate v) {
            d = v;
        } else if (value instanceof java.sql.Date v) {
            d = v.toLocalDate();
        } else if (value instanceof String s) {
            d = LocalDate.parse(s.trim());
        } else {
            throw new IllegalArgumentException(
                    "tier key value is not a date: " + value
                    + " (" + value.getClass().getName() + ")");
        }
        return d.toEpochDay();
    }

    private static String unquote(String text) {
        String t = text.trim();
        if (t.length() >= 2 && t.charAt(0) == '\'' && t.charAt(t.length() - 1) == '\'') {
            return t.substring(1, t.length() - 1);
        }
        return t;
    }

    private static long parseDuration(String text, long secondUnit, long dayUnit) {
        String t = text.trim().toLowerCase(Locale.ROOT);
        char suffix = t.charAt(t.length() - 1);
        if (Character.isDigit(suffix)) {
            return Long.parseLong(t);
        }
        long n = Long.parseLong(t.substring(0, t.length() - 1).trim());
        long unit = switch (suffix) {
            case 'd' -> dayUnit;
            case 'h' -> secondUnit * 3600L;
            case 'm' -> secondUnit * 60L;
            case 's' -> secondUnit;
            default -> throw new IllegalArgumentException("unknown duration suffix: " + text);
        };
        if (unit <= 0) {
            throw new IllegalArgumentException(
                    "duration '" + text + "' is finer than the tier key type supports");
        }
        return Math.multiplyExact(n, unit);
    }
}
