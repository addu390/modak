package io.tierdb.cdc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Builds pgoutput protocol-v1 wire messages byte-for-byte per the "Logical
 * Streaming Replication Protocol" spec, the same layout a live walsender emits,
 * so the decoder is tested against the real format without a running server.
 */
final class PgOutputFixtures {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    static ByteBuffer begin(long finalLsn, long commitMicros, int xid) {
        return new PgOutputFixtures().u8('B').u64(finalLsn).u64(commitMicros).u32(xid).buffer();
    }

    static ByteBuffer commit(long commitLsn, long endLsn, long commitMicros) {
        return new PgOutputFixtures().u8('C').u8(0).u64(commitLsn).u64(endLsn).u64(commitMicros)
                .buffer();
    }

    record Col(String name, boolean key, int typeOid, int typeMod) {
        Col(String name, boolean key, int typeOid) {
            this(name, key, typeOid, -1);
        }
    }

    static ByteBuffer relation(int oid, String schema, String name, char identity, List<Col> cols) {
        PgOutputFixtures f = new PgOutputFixtures()
                .u8('R').u32(oid).cstring(schema).cstring(name).u8(identity).u16(cols.size());
        for (Col c : cols) {
            f.u8(c.key() ? 1 : 0).cstring(c.name()).u32(c.typeOid()).u32(c.typeMod());
        }
        return f.buffer();
    }

    static final Object UNCHANGED = new Object();

    static ByteBuffer insert(int oid, Object... cells) {
        return new PgOutputFixtures().u8('I').u32(oid).u8('N').tuple(cells).buffer();
    }

    static ByteBuffer update(int oid, Object[] oldRow, Object[] newRow) {
        PgOutputFixtures f = new PgOutputFixtures().u8('U').u32(oid);
        if (oldRow != null) {
            f.u8('O').tuple(oldRow);
        }
        return f.u8('N').tuple(newRow).buffer();
    }

    static ByteBuffer delete(int oid, Object... oldRow) {
        return new PgOutputFixtures().u8('D').u32(oid).u8('O').tuple(oldRow).buffer();
    }

    static ByteBuffer truncate(int... oids) {
        PgOutputFixtures f = new PgOutputFixtures().u8('T').u32(oids.length).u8(0);
        for (int oid : oids) {
            f.u32(oid);
        }
        return f.buffer();
    }

    private PgOutputFixtures tuple(Object[] cells) {
        u16(cells.length);
        for (Object cell : cells) {
            if (cell == null) {
                u8('n');
            } else if (cell == UNCHANGED) {
                u8('u');
            } else {
                byte[] bytes = cell.toString().getBytes(StandardCharsets.UTF_8);
                u8('t').u32(bytes.length);
                out.writeBytes(bytes);
            }
        }
        return this;
    }

    private PgOutputFixtures u8(int v) {
        out.write(v & 0xFF);
        return this;
    }

    private PgOutputFixtures u16(int v) {
        out.write((v >>> 8) & 0xFF);
        out.write(v & 0xFF);
        return this;
    }

    private PgOutputFixtures u32(int v) {
        for (int shift = 24; shift >= 0; shift -= 8) {
            out.write((v >>> shift) & 0xFF);
        }
        return this;
    }

    private PgOutputFixtures u64(long v) {
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.write((int) ((v >>> shift) & 0xFF));
        }
        return this;
    }

    private PgOutputFixtures cstring(String s) {
        out.writeBytes(s.getBytes(StandardCharsets.UTF_8));
        out.write(0);
        return this;
    }

    private ByteBuffer buffer() {
        return ByteBuffer.wrap(out.toByteArray());
    }
}
