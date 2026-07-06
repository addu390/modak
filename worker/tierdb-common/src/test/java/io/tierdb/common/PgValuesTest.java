package io.tierdb.common;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PgValuesTest {

    @Test
    void mapsJdbcAndInformationSchemaSpellingsAlike() {
        assertEquals(ColumnType.LONG, PgValues.column("c", "int8", 0, 0).type());
        assertEquals(ColumnType.LONG, PgValues.column("c", "bigint", 0, 0).type());
        assertEquals(ColumnType.TIMESTAMP, PgValues.column("c", "timestamptz", 0, 0).type());
        assertEquals(ColumnType.TIMESTAMP,
                PgValues.column("c", "timestamp with time zone", 0, 0).type());
        assertEquals(ColumnType.DATE, PgValues.column("c", "date", 0, 0).type());
        assertEquals(ColumnType.UUID, PgValues.column("c", "uuid", 0, 0).type());
        assertEquals(ColumnType.BINARY, PgValues.column("c", "bytea", 0, 0).type());
        assertEquals(ColumnType.TEXT, PgValues.column("c", "jsonb", 0, 0).type());
    }

    @Test
    void constrainedNumericIsDecimalUnconstrainedRidesAsDouble() {
        assertEquals(new Column("c", ColumnType.DECIMAL, 10, 2),
                PgValues.column("c", "numeric", 10, 2));
        assertEquals(ColumnType.DOUBLE, PgValues.column("c", "numeric", 0, 0).type());
    }

    @Test
    void parsesPgTimestampTextWithAndWithoutOffset() {
        assertEquals(OffsetDateTime.of(2026, 1, 2, 3, 4, 5, 123_456_000, ZoneOffset.UTC),
                PgValues.parseTimestamp("2026-01-02 03:04:05.123456+00"));
        assertEquals(OffsetDateTime.of(2026, 1, 1, 21, 34, 5, 0, ZoneOffset.UTC),
                PgValues.parseTimestamp("2026-01-02 03:04:05+05:30"));
        assertEquals(OffsetDateTime.of(2026, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC),
                PgValues.parseTimestamp("2026-01-02 03:04:05"));
    }

    @Test
    void parsesTextValuesForTheNewTypes() {
        assertEquals(LocalDate.of(2026, 7, 1), PgValues.parseText("2026-07-01", ColumnType.DATE));
        assertEquals(new BigDecimal("12.50"), PgValues.parseText("12.50", ColumnType.DECIMAL));
        assertEquals(UUID.fromString("6f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"),
                PgValues.parseText("6f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0", ColumnType.UUID));
        assertArrayEquals(new byte[] {0x48, 0x69},
                (byte[]) PgValues.parseText("\\x4869", ColumnType.BINARY));
    }

    @Test
    void nonHexByteaFailsLoudly() {
        assertThrows(IllegalArgumentException.class,
                () -> PgValues.parseText("Hi", ColumnType.BINARY));
    }
}
