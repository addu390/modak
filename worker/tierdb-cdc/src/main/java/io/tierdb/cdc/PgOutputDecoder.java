package io.tierdb.cdc;

import io.tierdb.cdc.PgOutputMessage.Begin;
import io.tierdb.cdc.PgOutputMessage.Cell;
import io.tierdb.cdc.PgOutputMessage.Column;
import io.tierdb.cdc.PgOutputMessage.Commit;
import io.tierdb.cdc.PgOutputMessage.Delete;
import io.tierdb.cdc.PgOutputMessage.Insert;
import io.tierdb.cdc.PgOutputMessage.Relation;
import io.tierdb.cdc.PgOutputMessage.Skipped;
import io.tierdb.cdc.PgOutputMessage.Truncate;
import io.tierdb.cdc.PgOutputMessage.Update;
import io.tierdb.common.Lsn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Decodes {@code pgoutput} protocol-version-1 messages (text-mode tuples) from
 * the XLogData payload a replication stream hands back. Stateless, the buffer
 * must be positioned at the message-type byte (which is how PgJDBC delivers it).
 */
public final class PgOutputDecoder {

    private PgOutputDecoder() {}

    public static PgOutputMessage decode(ByteBuffer buf) {
        char type = (char) buf.get();
        return switch (type) {
            case 'B' -> new Begin(new Lsn(buf.getLong()), buf.getLong(),
                    Integer.toUnsignedLong(buf.getInt()));
            case 'C' -> commit(buf);
            case 'R' -> relation(buf);
            case 'I' -> insert(buf);
            case 'U' -> update(buf);
            case 'D' -> delete(buf);
            case 'T' -> truncate(buf);
            case 'O', 'Y', 'M' -> new Skipped(type);
            default -> throw new CdcException("unknown pgoutput message type: '" + type + "'");
        };
    }

    private static Commit commit(ByteBuffer buf) {
        buf.get();
        return new Commit(new Lsn(buf.getLong()), new Lsn(buf.getLong()), buf.getLong());
    }

    private static Relation relation(ByteBuffer buf) {
        int oid = buf.getInt();
        String schema = cstring(buf);
        String name = cstring(buf);
        char identity = (char) buf.get();
        int ncols = buf.getShort();
        List<Column> cols = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            boolean key = (buf.get() & 1) != 0;
            String colName = cstring(buf);
            int typeOid = buf.getInt();
            int typeMod = buf.getInt();
            cols.add(new Column(colName, key, typeOid, typeMod));
        }
        return new Relation(oid, schema, name, identity, List.copyOf(cols));
    }

    private static Insert insert(ByteBuffer buf) {
        int oid = buf.getInt();
        char marker = (char) buf.get();
        if (marker != 'N') {
            throw new CdcException("insert tuple marker expected 'N', got '" + marker + "'");
        }
        return new Insert(oid, tuple(buf));
    }

    private static Update update(ByteBuffer buf) {
        int oid = buf.getInt();
        char marker = (char) buf.get();
        List<Cell> oldRow = null;
        if (marker == 'K' || marker == 'O') {
            oldRow = tuple(buf);
            marker = (char) buf.get();
        }
        if (marker != 'N') {
            throw new CdcException("update tuple marker expected 'N', got '" + marker + "'");
        }
        return new Update(oid, oldRow, tuple(buf));
    }

    private static Delete delete(ByteBuffer buf) {
        int oid = buf.getInt();
        char marker = (char) buf.get();
        if (marker != 'K' && marker != 'O') {
            throw new CdcException("delete tuple marker expected 'K'/'O', got '" + marker + "'");
        }
        return new Delete(oid, tuple(buf));
    }

    private static Truncate truncate(ByteBuffer buf) {
        int nrels = buf.getInt();
        buf.get();
        List<Integer> oids = new ArrayList<>(nrels);
        for (int i = 0; i < nrels; i++) {
            oids.add(buf.getInt());
        }
        return new Truncate(List.copyOf(oids));
    }

    private static List<Cell> tuple(ByteBuffer buf) {
        int ncols = buf.getShort();
        List<Cell> cells = new ArrayList<>(ncols);
        for (int i = 0; i < ncols; i++) {
            char kind = (char) buf.get();
            switch (kind) {
                case 'n' -> cells.add(Cell.NULL_CELL);
                case 'u' -> cells.add(Cell.UNCHANGED);
                case 't' -> {
                    int len = buf.getInt();
                    byte[] bytes = new byte[len];
                    buf.get(bytes);
                    cells.add(Cell.of(new String(bytes, StandardCharsets.UTF_8)));
                }
                default -> throw new CdcException("unsupported tuple cell kind: '" + kind + "'");
            }
        }
        return List.copyOf(cells);
    }

    private static String cstring(ByteBuffer buf) {
        int start = buf.position();
        while (buf.get() != 0) {
        }
        int len = buf.position() - start - 1;
        byte[] bytes = new byte[len];
        buf.position(start);
        buf.get(bytes);
        buf.get();
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
