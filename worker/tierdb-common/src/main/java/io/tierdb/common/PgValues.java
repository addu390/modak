package io.tierdb.common;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.HexFormat;
import java.util.TimeZone;
import java.util.UUID;

/**
 * The single mapping between Postgres types/values and {@link ColumnType}, shared
 * by every ingestion path. Type names are accepted in both spellings: JDBC
 * metadata ({@code int8}) and information_schema ({@code bigint}).
 */
public final class PgValues {

    private PgValues() {}

    private static final DateTimeFormatter PG_TIMESTAMP = new DateTimeFormatterBuilder()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .appendPattern("HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH:mm:ss", "Z")
            .optionalEnd()
            .toFormatter();

    public static Column column(String name, String pgTypeName, int precision, int scale) {
        ColumnType type = switch (pgTypeName) {
            case "bool", "boolean" -> ColumnType.BOOLEAN;
            case "int2", "smallint", "int4", "integer", "int8", "bigint", "oid" -> ColumnType.LONG;
            case "float4", "real", "float8", "double precision" -> ColumnType.DOUBLE;
            case "numeric", "decimal" -> precision > 0 ? ColumnType.DECIMAL : ColumnType.DOUBLE;
            case "timestamp", "timestamp without time zone",
                    "timestamptz", "timestamp with time zone" -> ColumnType.TIMESTAMP;
            case "date" -> ColumnType.DATE;
            case "uuid" -> ColumnType.UUID;
            case "bytea" -> ColumnType.BINARY;
            default -> ColumnType.TEXT;
        };
        return type == ColumnType.DECIMAL
                ? new Column(name, type, precision, scale)
                : new Column(name, type);
    }

    public static Object readValue(ResultSet rs, int idx, ColumnType type) throws SQLException {
        Object v = switch (type) {
            case LONG -> rs.getLong(idx);
            case DOUBLE -> rs.getDouble(idx);
            case BOOLEAN -> rs.getBoolean(idx);
            case TEXT -> rs.getString(idx);
            case TIMESTAMP -> toUtc(rs.getTimestamp(idx,
                    Calendar.getInstance(TimeZone.getTimeZone("UTC"))));
            case DATE -> rs.getObject(idx, LocalDate.class);
            case DECIMAL -> rs.getBigDecimal(idx);
            case UUID -> rs.getObject(idx);
            case BINARY -> rs.getBytes(idx);
        };
        return rs.wasNull() ? null : v;
    }

    public static String castSuffix(ColumnType type) {
        return switch (type) {
            case LONG -> "::bigint";
            case DOUBLE -> "::double precision";
            case BOOLEAN -> "::boolean";
            case TEXT -> "";
            case TIMESTAMP -> "::timestamptz";
            case DATE -> "::date";
            case DECIMAL -> "::numeric";
            case UUID -> "::uuid";
            case BINARY -> "::bytea";
        };
    }

    public static Object parseText(String text, ColumnType type) {
        return switch (type) {
            case LONG -> Long.parseLong(text);
            case DOUBLE -> Double.parseDouble(text);
            case BOOLEAN -> text.equals("t") || text.equals("true");
            case TEXT -> text;
            case TIMESTAMP -> parseTimestamp(text);
            case DATE -> LocalDate.parse(text);
            case DECIMAL -> new BigDecimal(text);
            case UUID -> UUID.fromString(text);
            case BINARY -> parseBytea(text);
        };
    }

    public static OffsetDateTime parseTimestamp(String text) {
        if (text.length() > 10 && text.charAt(10) == 'T') {
            text = text.substring(0, 10) + ' ' + text.substring(11);
        }
        TemporalAccessor parsed = PG_TIMESTAMP.parseBest(
                text, OffsetDateTime::from, LocalDateTime::from);
        return parsed instanceof OffsetDateTime odt
                ? odt.withOffsetSameInstant(ZoneOffset.UTC)
                : ((LocalDateTime) parsed).atOffset(ZoneOffset.UTC);
    }

    private static byte[] parseBytea(String text) {
        if (!text.startsWith("\\x")) {
            throw new IllegalArgumentException(
                    "bytea text is not in hex format (bytea_output = 'hex' required)");
        }
        return HexFormat.of().parseHex(text, 2, text.length());
    }

    private static OffsetDateTime toUtc(Timestamp ts) {
        return ts == null ? null : ts.toInstant().atOffset(ZoneOffset.UTC);
    }
}
