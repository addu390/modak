package io.modak.cdc;

import io.modak.common.Lsn;
import java.util.List;

/**
 * One decoded {@code pgoutput} protocol message (protocol version 1). The wire
 * format is documented under "Logical Streaming Replication Protocol" in the
 * Postgres docs. {@link PgOutputDecoder} produces these from the raw XLogData
 * payload of a replication stream.
 */
public sealed interface PgOutputMessage {

    /** One column of a {@link Relation}: name, replica-identity membership, type OID and typmod. */
    record Column(String name, boolean key, int typeOid, int typeMod) {}

    enum CellKind {
        NULL,
        TEXT,
        /** A TOASTed value the change did not touch, so the image is only in the old tuple. */
        UNCHANGED_TOAST
    }

    /** One column value of a tuple, text-mode. */
    record Cell(CellKind kind, String text) {
        public static final Cell NULL_CELL = new Cell(CellKind.NULL, null);
        public static final Cell UNCHANGED = new Cell(CellKind.UNCHANGED_TOAST, null);

        public static Cell of(String text) {
            return new Cell(CellKind.TEXT, text);
        }
    }

    record Begin(Lsn finalLsn, long commitTimeMicros, long xid) implements PgOutputMessage {}

    record Commit(Lsn commitLsn, Lsn endLsn, long commitTimeMicros) implements PgOutputMessage {}

    /** Schema announcement, sent before the first change of a relation on each stream. */
    record Relation(
            int relationOid,
            String schemaName,
            String tableName,
            char replicaIdentity,
            List<Column> columns) implements PgOutputMessage {}

    record Insert(int relationOid, List<Cell> newRow) implements PgOutputMessage {}

    /** {@code oldRow} is null unless the table has REPLICA IDENTITY FULL / the key changed. */
    record Update(int relationOid, List<Cell> oldRow, List<Cell> newRow) implements PgOutputMessage {}

    record Delete(int relationOid, List<Cell> oldRow) implements PgOutputMessage {}

    record Truncate(List<Integer> relationOids) implements PgOutputMessage {}

    /** Origin / Type / Message, irrelevant to mirroring and safely skipped. */
    record Skipped(char type) implements PgOutputMessage {}
}
