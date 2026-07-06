package io.tierdb.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class TierKeyTypeTest {

    @Test
    void forTypeAcceptsBothSpellings() {
        assertEquals(TierKeyType.BIGINT, TierKeyType.forType("int8"));
        assertEquals(TierKeyType.BIGINT, TierKeyType.forType("bigint"));
        assertEquals(TierKeyType.BIGINT, TierKeyType.forType("integer"));
        assertEquals(TierKeyType.TIMESTAMPTZ, TierKeyType.forType("timestamptz"));
        assertEquals(TierKeyType.TIMESTAMPTZ, TierKeyType.forType("timestamp with time zone"));
        assertEquals(TierKeyType.TIMESTAMP, TierKeyType.forType("timestamp without time zone"));
        assertEquals(TierKeyType.DATE, TierKeyType.forType("date"));
        assertThrows(IllegalArgumentException.class, () -> TierKeyType.forType("text"));
    }

    @Test
    void identityRoundTrip() {
        assertEquals(42L, TierKeyType.BIGINT.encode(42L));
        assertEquals(42L, TierKeyType.BIGINT.encode("42"));
        assertEquals(42L, TierKeyType.BIGINT.decode(42L));
        assertEquals("42", TierKeyType.BIGINT.pgLiteral(42L));
        assertEquals(100L, TierKeyType.BIGINT.parseBoundLiteral("'100'"));
        assertEquals(-5L, TierKeyType.BIGINT.parseBoundLiteral("-5"));
        assertEquals("m5", TierKeyType.BIGINT.nameToken(-5L));
        assertEquals("100", TierKeyType.BIGINT.nameToken(100L));
    }

    @Test
    void timestampRoundTripPreservesMicros() {
        OffsetDateTime odt = OffsetDateTime.of(2026, 7, 1, 12, 30, 45, 123_456_000, ZoneOffset.UTC);
        long canonical = TierKeyType.TIMESTAMPTZ.encode(odt);
        assertEquals(odt, TierKeyType.TIMESTAMPTZ.decode(canonical));
        assertEquals(canonical, TierKeyType.TIMESTAMPTZ.encode("2026-07-01 12:30:45.123456+00"));
        assertEquals("TIMESTAMPTZ '2026-07-01 12:30:45.123456+00'",
                TierKeyType.TIMESTAMPTZ.pgLiteral(canonical));
    }

    @Test
    void timestampOrderingIsPreserved() {
        long a = TierKeyType.TIMESTAMPTZ.encode("2026-01-01 00:00:00+00");
        long b = TierKeyType.TIMESTAMPTZ.encode("2026-01-01 00:00:00.000001+00");
        long c = TierKeyType.TIMESTAMPTZ.encode("1969-12-31 23:59:59+00");
        assertTrue(a < b);
        assertTrue(c < 0);
        assertTrue(c < a);
    }

    @Test
    void timestampBoundAndToken() {
        long canonical = TierKeyType.TIMESTAMPTZ.parseBoundLiteral("'2026-07-01 00:00:00+00'");
        assertEquals("20260701T000000", TierKeyType.TIMESTAMPTZ.nameToken(canonical));
        assertEquals(canonical, TierKeyType.TIMESTAMPTZ.encode("2026-07-01 00:00:00+00"));
    }

    @Test
    void plainTimestampLiteralHasNoOffset() {
        long canonical = TierKeyType.TIMESTAMP.encode("2026-07-01 00:00:00");
        assertEquals("TIMESTAMP '2026-07-01 00:00:00.000000'", TierKeyType.TIMESTAMP.pgLiteral(canonical));
    }

    @Test
    void dateRoundTrip() {
        LocalDate d = LocalDate.of(2026, 7, 1);
        long canonical = TierKeyType.DATE.encode(d);
        assertEquals(d.toEpochDay(), canonical);
        assertEquals(d, TierKeyType.DATE.decode(canonical));
        assertEquals("DATE '2026-07-01'", TierKeyType.DATE.pgLiteral(canonical));
        assertEquals(canonical, TierKeyType.DATE.parseBoundLiteral("'2026-07-01'"));
        assertEquals("20260701", TierKeyType.DATE.nameToken(canonical));
    }

    @Test
    void lagParsing() {
        assertEquals(100L, TierKeyType.BIGINT.parseLagOrWidth("100"));
        assertEquals(90L * 86_400_000_000L, TierKeyType.TIMESTAMPTZ.parseLagOrWidth("90d"));
        assertEquals(12L * 3_600_000_000L, TierKeyType.TIMESTAMPTZ.parseLagOrWidth("12h"));
        assertEquals(30L * 60_000_000L, TierKeyType.TIMESTAMPTZ.parseLagOrWidth("30m"));
        assertEquals(45_000_000L, TierKeyType.TIMESTAMPTZ.parseLagOrWidth("45s"));
        assertEquals(90L, TierKeyType.DATE.parseLagOrWidth("90d"));
        assertThrows(IllegalArgumentException.class, () -> TierKeyType.DATE.parseLagOrWidth("12h"));
    }

    @Test
    void encodeRejectsWrongShapes() {
        assertThrows(IllegalArgumentException.class, () -> TierKeyType.TIMESTAMPTZ.encode(42L));
        assertThrows(IllegalArgumentException.class, () -> TierKeyType.DATE.encode(42L));
        assertThrows(IllegalArgumentException.class, () -> TierKeyType.BIGINT.encode(LocalDate.now()));
    }
}
