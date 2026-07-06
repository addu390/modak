package io.tierdb.cdc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.cdc.PgOutputMessage.Begin;
import io.tierdb.cdc.PgOutputMessage.Cell;
import io.tierdb.cdc.PgOutputMessage.CellKind;
import io.tierdb.cdc.PgOutputMessage.Commit;
import io.tierdb.cdc.PgOutputMessage.Delete;
import io.tierdb.cdc.PgOutputMessage.Insert;
import io.tierdb.cdc.PgOutputMessage.Relation;
import io.tierdb.cdc.PgOutputMessage.Skipped;
import io.tierdb.cdc.PgOutputMessage.Truncate;
import io.tierdb.cdc.PgOutputMessage.Update;
import io.tierdb.common.Lsn;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class PgOutputDecoderTest {

    private static final int VEHICLES_OID = 90210;

    @Test
    void decodesBeginWithLsnTimestampAndXid() {
        Begin b = (Begin) PgOutputDecoder.decode(PgOutputFixtures.begin(0x1_0000_00AAL, 777L, 42));
        assertEquals(new Lsn(0x1_0000_00AAL), b.finalLsn());
        assertEquals(777L, b.commitTimeMicros());
        assertEquals(42L, b.xid());
    }

    @Test
    void decodesCommitWithBothLsns() {
        Commit c = (Commit) PgOutputDecoder.decode(PgOutputFixtures.commit(100L, 108L, 999L));
        assertEquals(new Lsn(100L), c.commitLsn());
        assertEquals(new Lsn(108L), c.endLsn());
        assertEquals(999L, c.commitTimeMicros());
    }

    @Test
    void decodesRelationSchemaNameIdentityAndColumns() {
        Relation r = (Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                VEHICLES_OID, "public", "vehicles", 'f',
                List.of(new PgOutputFixtures.Col("vin", true, 25),
                        new PgOutputFixtures.Col("odometer", false, 20))));
        assertEquals(VEHICLES_OID, r.relationOid());
        assertEquals("public", r.schemaName());
        assertEquals("vehicles", r.tableName());
        assertEquals('f', r.replicaIdentity());
        assertEquals(2, r.columns().size());
        assertEquals(new PgOutputMessage.Column("vin", true, 25, -1), r.columns().get(0));
        assertEquals(new PgOutputMessage.Column("odometer", false, 20, -1), r.columns().get(1));
    }

    @Test
    void decodesInsertTextAndNullCells() {
        Insert i = (Insert) PgOutputDecoder.decode(
                PgOutputFixtures.insert(VEHICLES_OID, "VIN-1", null, "12000"));
        assertEquals(VEHICLES_OID, i.relationOid());
        assertEquals(Cell.of("VIN-1"), i.newRow().get(0));
        assertEquals(CellKind.NULL, i.newRow().get(1).kind());
        assertEquals(Cell.of("12000"), i.newRow().get(2));
    }

    @Test
    void decodesUpdateWithOldImageAndUnchangedToast() {
        Update u = (Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                VEHICLES_OID,
                new Object[] {"VIN-1", "big-blob", "12000"},
                new Object[] {"VIN-1", PgOutputFixtures.UNCHANGED, "13000"}));
        assertEquals(Cell.of("big-blob"), u.oldRow().get(1));
        assertEquals(CellKind.UNCHANGED_TOAST, u.newRow().get(1).kind());
        assertEquals(Cell.of("13000"), u.newRow().get(2));
    }

    @Test
    void decodesUpdateWithoutOldImage() {
        Update u = (Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                VEHICLES_OID, null, new Object[] {"VIN-1", "x", "13000"}));
        assertNull(u.oldRow());
        assertEquals(Cell.of("13000"), u.newRow().get(2));
    }

    @Test
    void decodesDeleteWithOldImage() {
        Delete d = (Delete) PgOutputDecoder.decode(
                PgOutputFixtures.delete(VEHICLES_OID, "VIN-1", "x", "12000"));
        assertEquals(VEHICLES_OID, d.relationOid());
        assertEquals(Cell.of("VIN-1"), d.oldRow().get(0));
    }

    @Test
    void decodesTruncateRelationList() {
        Truncate t = (Truncate) PgOutputDecoder.decode(PgOutputFixtures.truncate(1, 2, 3));
        assertEquals(List.of(1, 2, 3), t.relationOids());
    }

    @Test
    void skipsOriginTypeAndMessageKinds() {
        for (char kind : new char[] {'O', 'Y', 'M'}) {
            PgOutputMessage m = PgOutputDecoder.decode(ByteBuffer.wrap(new byte[] {(byte) kind}));
            assertTrue(m instanceof Skipped s && s.type() == kind);
        }
    }

    @Test
    void unknownMessageTypeFailsLoudly() {
        assertThrows(CdcException.class,
                () -> PgOutputDecoder.decode(ByteBuffer.wrap(new byte[] {'Z'})));
    }

    @Test
    void utf8SurvivesIdentifiersAndValues() {
        Relation r = (Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                7, "public", "véhicules", 'd',
                List.of(new PgOutputFixtures.Col("clé", true, 25))));
        assertEquals("véhicules", r.tableName());
        assertEquals("clé", r.columns().get(0).name());

        Insert i = (Insert) PgOutputDecoder.decode(PgOutputFixtures.insert(7, "Škoda Octavia"));
        assertEquals("Škoda Octavia", i.newRow().get(0).text());
    }
}
