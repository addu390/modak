package io.tierdb.common;

/**
 * A Postgres WAL log-sequence-number, the mirror frontier's clock. Stored as
 * the raw 64-bit position ({@code pg_lsn} cast to bigint). {@link #toPg()}
 * renders the familiar {@code XXX/YYY} form for replication calls and logs.
 */
public record Lsn(long value) implements Comparable<Lsn> {

    public static final Lsn ZERO = new Lsn(0);

    @Override
    public int compareTo(Lsn o) {
        return Long.compareUnsigned(this.value, o.value);
    }

    public String toPg() {
        return String.format("%X/%X", value >>> 32, value & 0xFFFFFFFFL);
    }

    public static Lsn fromPg(String text) {
        int slash = text.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("not a pg LSN: " + text);
        }
        long hi = Long.parseLong(text, 0, slash, 16);
        long lo = Long.parseLong(text, slash + 1, text.length(), 16);
        return new Lsn((hi << 32) | lo);
    }

    @Override
    public String toString() {
        return "Lsn[" + toPg() + "]";
    }
}
