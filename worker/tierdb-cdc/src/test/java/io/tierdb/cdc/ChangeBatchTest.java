package io.tierdb.cdc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.cdc.PgOutputMessage.Delete;
import io.tierdb.cdc.PgOutputMessage.Insert;
import io.tierdb.cdc.PgOutputMessage.Relation;
import io.tierdb.cdc.PgOutputMessage.Update;
import io.tierdb.common.DeltaRowsBatch;
import io.tierdb.common.RowBatchData;
import io.tierdb.common.TableId;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ChangeBatchTest {

    private static final int OID = 90210;
    private static final TableId TABLE = new TableId(OID);

    private ChangeBatch batch;

    @BeforeEach
    void setUp() {
        batch = new ChangeBatch(TABLE, List.of("vin"), "updated_at");
        batch.onRelation((Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                OID, "public", "vehicles", 'f',
                List.of(new PgOutputFixtures.Col("vin", true, 25),        // text
                        new PgOutputFixtures.Col("model", false, 25),     // text
                        new PgOutputFixtures.Col("odometer", false, 20),  // int8
                        new PgOutputFixtures.Col("updated_at", false, 20)))));
    }

    private void insert(String vin, String model, long odometer, long updatedAt) {
        batch.onInsert((Insert) PgOutputDecoder.decode(
                PgOutputFixtures.insert(OID, vin, model, odometer, updatedAt)));
    }

    @Test
    void insertBecomesAnUpsertEntryWithTypedRowAndTierKey() {
        insert("VIN-1", "Model 3", 12000, 1000);

        DeltaRowsBatch out = batch.drain();
        assertEquals(TABLE, out.table());
        assertEquals(List.of("vin"), out.pkColumns());
        assertEquals(new RowBatchData.Column("odometer", RowBatchData.ColumnType.LONG),
                out.columns().get(2));

        DeltaRowsBatch.Entry e = out.entries().get(0);
        assertEquals("VIN-1", e.pk());
        assertFalse(e.tombstone());
        assertEquals(1000, e.tierKey());
        assertEquals(12000L, e.row()[2]);
    }

    @Test
    void newestWinsPerPkWithinTheBatch() {
        insert("VIN-1", "Model 3", 12000, 1000);
        insert("VIN-2", "Corolla", 500, 1001);
        batch.onUpdate((Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                OID,
                new Object[] {"VIN-1", "Model 3", 12000, 1000},
                new Object[] {"VIN-1", "Model 3", 12500, 1002})));

        DeltaRowsBatch out = batch.drain();
        assertEquals(2, out.entries().size());
        DeltaRowsBatch.Entry vin1 = out.entries().stream()
                .filter(e -> e.pk().equals("VIN-1")).findFirst().orElseThrow();
        assertEquals(12500L, vin1.row()[2]);
        assertEquals(1002, vin1.tierKey());
    }

    @Test
    void anUpdateMovingTheTierKeyRemembersTheLakePartition() {
        batch.onUpdate((Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                OID,
                new Object[] {"VIN-1", "Model 3", 12000, 1000},
                new Object[] {"VIN-1", "Model 3", 12500, 1002})));

        DeltaRowsBatch.Entry e = batch.drain().entries().get(0);
        assertEquals(1002, e.tierKey());
        assertEquals(1000L, e.oldTierKey(), "the lake still holds the image at the old tier");
        assertEquals(1000L, e.lakeTierKey());
    }

    @Test
    void collapsedMovesKeepTheOriginalLakePartition() {
        batch.onUpdate((Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                OID,
                new Object[] {"VIN-1", "Model 3", 12000, 1000},
                new Object[] {"VIN-1", "Model 3", 12500, 1002})));
        batch.onUpdate((Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                OID,
                new Object[] {"VIN-1", "Model 3", 12500, 1002},
                new Object[] {"VIN-1", "Model 3", 13000, 1005})));

        DeltaRowsBatch.Entry e = batch.drain().entries().get(0);
        assertEquals(1005, e.tierKey());
        assertEquals(1000L, e.lakeTierKey());
    }

    @Test
    void deleteBecomesATombstoneCarryingTheOldImage() {
        batch.onDelete((Delete) PgOutputDecoder.decode(PgOutputFixtures.delete(
                OID, "VIN-1", "Model 3", 12000, 1000)));

        DeltaRowsBatch.Entry e = batch.drain().entries().get(0);
        assertEquals("VIN-1", e.pk());
        assertTrue(e.tombstone());
        assertEquals(1000, e.tierKey());
        assertEquals("VIN-1", e.row()[0]);
    }

    @Test
    void insertThenDeleteOfSamePkCollapsesToTheTombstone() {
        insert("VIN-1", "Model 3", 12000, 1000);
        batch.onDelete((Delete) PgOutputDecoder.decode(PgOutputFixtures.delete(
                OID, "VIN-1", "Model 3", 12000, 1000)));

        DeltaRowsBatch out = batch.drain();
        assertEquals(1, out.entries().size());
        assertTrue(out.entries().get(0).tombstone());
    }

    @Test
    void unchangedToastCellIsFilledFromTheOldImage() {
        batch.onUpdate((Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                OID,
                new Object[] {"VIN-1", "big-toasted-model-name", 12000, 1000},
                new Object[] {"VIN-1", PgOutputFixtures.UNCHANGED, 12500, 1002})));

        DeltaRowsBatch.Entry e = batch.drain().entries().get(0);
        assertEquals("big-toasted-model-name", e.row()[1]);
        assertEquals(12500L, e.row()[2]);
    }

    @Test
    void unchangedToastWithoutOldImageFailsLoudly() {
        assertThrows(CdcException.class, () -> batch.onUpdate(
                (Update) PgOutputDecoder.decode(PgOutputFixtures.update(
                        OID, null,
                        new Object[] {"VIN-1", PgOutputFixtures.UNCHANGED, 12500, 1002}))));
    }

    @Test
    void drainClearsTheAccumulator() {
        insert("VIN-1", "Model 3", 12000, 1000);
        assertEquals(1, batch.size());
        batch.drain();
        assertTrue(batch.isEmpty());
    }

    @Test
    void changesBeforeARelationMessageFailLoudly() {
        ChangeBatch fresh = new ChangeBatch(TABLE, List.of("vin"), "updated_at");
        assertThrows(CdcException.class, () -> fresh.onInsert(
                (Insert) PgOutputDecoder.decode(PgOutputFixtures.insert(OID, "VIN-1"))));
    }

    @Test
    void relationMissingPkOrTierKeyIsRejected() {
        ChangeBatch fresh = new ChangeBatch(TABLE, List.of("vin"), "updated_at");
        assertThrows(CdcException.class, () -> fresh.onRelation(
                (Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                        OID, "public", "vehicles", 'f',
                        List.of(new PgOutputFixtures.Col("other", true, 25))))));
    }

    private Relation relationOf(PgOutputFixtures.Col... cols) {
        return (Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                OID, "public", "vehicles", 'f', List.of(cols)));
    }

    private static final PgOutputFixtures.Col VIN = new PgOutputFixtures.Col("vin", true, 25);
    private static final PgOutputFixtures.Col MODEL = new PgOutputFixtures.Col("model", false, 25);
    private static final PgOutputFixtures.Col ODO = new PgOutputFixtures.Col("odometer", false, 20);
    private static final PgOutputFixtures.Col TS = new PgOutputFixtures.Col("updated_at", false, 20);

    @Test
    void classifyReturnsAllColumnsOnTheFirstRelation() {
        ChangeBatch fresh = new ChangeBatch(TABLE, List.of("vin"), "updated_at");
        List<RowBatchData.Column> all = fresh.classify(relationOf(VIN, MODEL, ODO, TS));
        assertEquals(4, all.size(), "first Relation: everything, for an idempotent reconcile");
        assertEquals("vin", all.get(0).name());
    }

    @Test
    void classifyIsEmptyWhenTheLayoutIsUnchanged() {
        assertTrue(batch.classify(relationOf(VIN, MODEL, ODO, TS)).isEmpty());
    }

    @Test
    void classifyReportsAppendedColumnsAsAdditive() {
        List<RowBatchData.Column> added = batch.classify(relationOf(VIN, MODEL, ODO, TS,
                new PgOutputFixtures.Col("color", false, 25)));
        assertEquals(1, added.size());
        assertEquals(new RowBatchData.Column("color", RowBatchData.ColumnType.TEXT), added.get(0));
    }

    @Test
    void classifyRejectsDroppedColumns() {
        SchemaDivergedException e = assertThrows(SchemaDivergedException.class,
                () -> batch.classify(relationOf(VIN, MODEL, ODO)));
        assertTrue(e.getMessage().contains("updated_at"), e.getMessage());
    }

    @Test
    void classifyRejectsRenames() {
        SchemaDivergedException e = assertThrows(SchemaDivergedException.class,
                () -> batch.classify(relationOf(VIN,
                        new PgOutputFixtures.Col("model_name", false, 25), ODO, TS)));
        assertTrue(e.getMessage().contains("model"), e.getMessage());
    }

    @Test
    void classifyRejectsTypeChangesAcrossFamilies() {
        SchemaDivergedException e = assertThrows(SchemaDivergedException.class,
                () -> batch.classify(relationOf(VIN, MODEL,
                        new PgOutputFixtures.Col("odometer", false, 25), TS)));
        assertTrue(e.getMessage().contains("odometer"), e.getMessage());
    }

    @Test
    void classifyToleratesTypeChangesWithinTheSameFamily() {
        assertTrue(batch.classify(relationOf(VIN, MODEL,
                new PgOutputFixtures.Col("odometer", false, 23), TS)).isEmpty());
    }

    @Test
    void typedColumnsDecodeToCanonicalValues() {
        ChangeBatch typed = new ChangeBatch(TABLE, List.of("id"), "updated_at");
        typed.onRelation((Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                OID, "public", "readings", 'f',
                List.of(new PgOutputFixtures.Col("id", true, 2950),          // uuid
                        new PgOutputFixtures.Col("taken_at", false, 1184),   // timestamptz
                        new PgOutputFixtures.Col("day", false, 1082),        // date
                        new PgOutputFixtures.Col("amount", false, 1700, ((10 << 16) | 2) + 4),
                        new PgOutputFixtures.Col("blob", false, 17),         // bytea
                        new PgOutputFixtures.Col("updated_at", false, 20)))));

        typed.onInsert((Insert) PgOutputDecoder.decode(PgOutputFixtures.insert(OID,
                "6f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0",
                "2026-01-02 03:04:05.5+00", "2026-07-01", "12.50", "\\x4869", 1000L)));

        DeltaRowsBatch out = typed.drain();
        assertEquals(new RowBatchData.Column("amount", RowBatchData.ColumnType.DECIMAL, 10, 2),
                out.columns().get(3));
        DeltaRowsBatch.Entry e = out.entries().get(0);
        assertEquals(java.util.UUID.fromString("6f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"), e.row()[0]);
        assertEquals(java.time.OffsetDateTime.of(2026, 1, 2, 3, 4, 5, 500_000_000,
                java.time.ZoneOffset.UTC), e.row()[1]);
        assertEquals(java.time.LocalDate.of(2026, 7, 1), e.row()[2]);
        assertEquals(new java.math.BigDecimal("12.50"), e.row()[3]);
        assertArrayEquals(new byte[] {0x48, 0x69}, (byte[]) e.row()[4]);
        assertEquals(1000L, e.tierKey());
    }

    @Test
    void unconstrainedNumericStaysDoubleForBackwardsCompatibleSchemas() {
        ChangeBatch typed = new ChangeBatch(TABLE, List.of("vin"), "updated_at");
        typed.onRelation((Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                OID, "public", "vehicles", 'f',
                List.of(new PgOutputFixtures.Col("vin", true, 25),
                        new PgOutputFixtures.Col("price", false, 1700),  // numeric, no typmod
                        new PgOutputFixtures.Col("updated_at", false, 20)))));
        typed.onInsert((Insert) PgOutputDecoder.decode(
                PgOutputFixtures.insert(OID, "VIN-1", "19999.5", 1000L)));

        DeltaRowsBatch out = typed.drain();
        assertEquals(RowBatchData.ColumnType.DOUBLE, out.columns().get(1).type());
        assertEquals(19999.5, out.entries().get(0).row()[1]);
    }

    @Test
    void compositePkKeysOnTheCanonicalJoinedEncoding() {
        ChangeBatch composite = new ChangeBatch(TABLE, List.of("vin", "model"), "updated_at");
        composite.onRelation((Relation) PgOutputDecoder.decode(PgOutputFixtures.relation(
                OID, "public", "vehicles", 'f',
                List.of(new PgOutputFixtures.Col("vin", true, 25),
                        new PgOutputFixtures.Col("model", true, 25),
                        new PgOutputFixtures.Col("odometer", false, 20),
                        new PgOutputFixtures.Col("updated_at", false, 20)))));

        composite.onInsert((Insert) PgOutputDecoder.decode(
                PgOutputFixtures.insert(OID, "VIN-1", "Model 3", 100L, 1000L)));
        composite.onInsert((Insert) PgOutputDecoder.decode(
                PgOutputFixtures.insert(OID, "VIN-1", "Corolla", 200L, 1001L)));
        composite.onInsert((Insert) PgOutputDecoder.decode(
                PgOutputFixtures.insert(OID, "VIN-1", "Model 3", 300L, 1002L)));

        DeltaRowsBatch out = composite.drain();
        assertEquals(List.of("vin", "model"), out.pkColumns());
        assertEquals(2, out.entries().size(), "same (vin, model) collapses; different model doesn't");
        DeltaRowsBatch.Entry m3 = out.entries().stream()
                .filter(e -> e.pk().equals("VIN-1\u001fModel 3")).findFirst().orElseThrow();
        assertEquals(300L, m3.row()[2]);
    }
}
