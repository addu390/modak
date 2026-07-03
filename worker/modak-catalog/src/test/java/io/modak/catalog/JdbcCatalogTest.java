package io.modak.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.modak.common.Cutline;
import io.modak.common.DeltaBatch;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link JdbcCatalog} against an embedded Postgres with the
 * canonical catalog DDL applied.
 */
class JdbcCatalogTest {

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;

    private JdbcCatalog catalog;
    private TableId table;

    @BeforeAll
    static void startPostgres() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);
    }

    @AfterAll
    static void stopPostgres() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @BeforeEach
    void reset() {
        exec("TRUNCATE modak.tables CASCADE");
        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                42L, "public", "events",
                List.of("id"), "event_time",
                "{\"unit\":\"hour\"}", "iceberg", "warehouse.public.events",
                "{\"catalog\":\"rest\"}"));
        catalog.initCutline(table, new TierKey(1000), new LakeSnapshotId(1));
    }

    private record TestBatch(TableId table, int size, List<DeltaBatch.Key> keys)
            implements DeltaBatch {}

    @Test
    void registersAndReadsBackAllFields() {
        var found = catalog.lookup("public", "events");
        assertTrue(found.isPresent());
        RegisteredTable t = found.get();
        assertEquals(table, t.id());
        assertEquals(List.of("id"), t.primaryKeyCols());
        assertEquals("event_time", t.tierKeyCol());
        assertEquals("iceberg", t.lakeFormat());
        assertEquals("warehouse.public.events", t.lakeTableRef());
        assertEquals(new LakeSnapshotId(1), catalog.readCutline(table).snapshot());
    }

    @Test
    void tieredRegistrationDefaultsModeAndLeavesMirrorFieldsEmpty() {
        RegisteredTable t = catalog.get(table).orElseThrow();
        assertEquals(TableMode.TIERED, t.mode());
        assertEquals(null, t.publicationName());
        assertEquals(null, t.slotName());
        assertFalse(t.heapRetentionLag().isPresent());
        assertFalse(catalog.readMirrorFrontier(table).isPresent());
    }

    @Test
    void mirroredRegistrationRoundTripsModePublicationSlotAndRetention() {
        TableId mirrored = catalog.register(new TableRegistration(
                77L, "public", "vehicles", List.of("vin"), "updated_at",
                "{}", "iceberg", "warehouse.public.vehicles", null,
                TableMode.MIRRORED, "modak_pub_vehicles", "modak_slot_vehicles",
                java.util.Optional.of(3_600_000_000L), java.util.Optional.empty()));

        RegisteredTable t = catalog.get(mirrored).orElseThrow();
        assertEquals(TableMode.MIRRORED, t.mode());
        assertEquals("modak_pub_vehicles", t.publicationName());
        assertEquals("modak_slot_vehicles", t.slotName());
        assertEquals(3_600_000_000L, t.heapRetentionLag().orElseThrow());
        assertTrue(t.dropsHeapPartitions());
    }

    @Test
    void mirrorFrontierSeedsFromNullThenAdvancesMonotonically() {
        // The copy seeds a null frontier, every commit after moves (LSN, S) together.
        catalog.advanceMirrorFrontier(table, new Lsn(100), new LakeSnapshotId(2),
                java.util.Map.of("metadata_location", "/wh/events/metadata/00002-m.metadata.json"));
        assertEquals(new Lsn(100), catalog.readMirrorFrontier(table).orElseThrow());
        assertEquals(new LakeSnapshotId(2), catalog.readCutline(table).snapshot());
        assertTrue(catalog.get(table).orElseThrow().lakeProps().contains("00002-m.metadata.json"));

        assertThrows(CatalogException.class,
                () -> catalog.advanceMirrorFrontier(table, new Lsn(50), new LakeSnapshotId(3),
                        java.util.Map.of("metadata_location", "/wh/should-not-appear.json")));
        assertThrows(CatalogException.class,
                () -> catalog.advanceMirrorFrontier(table, new Lsn(200), new LakeSnapshotId(1),
                        java.util.Map.of()));
        assertEquals(new Lsn(100), catalog.readMirrorFrontier(table).orElseThrow());
        assertFalse(catalog.get(table).orElseThrow().lakeProps().contains("should-not-appear"));
    }

    @Test
    void lookupOfUnknownTableIsEmpty() {
        assertFalse(catalog.lookup("public", "nope").isPresent());
    }

    @Test
    void duplicateRegistrationRejectedByUniqueConstraint() {
        assertThrows(CatalogException.class, () -> catalog.register(new TableRegistration(
                99L, "public", "events", List.of("id"), "event_time",
                "{}", "iceberg", "warehouse.public.events", null)));
    }

    @Test
    void advancesCutlineAtomicallyAndMonotonically() {
        catalog.advanceCutline(table, new TierKey(2000), new LakeSnapshotId(5));
        assertEquals(new Cutline(new TierKey(2000), new LakeSnapshotId(5)), catalog.readCutline(table));

        assertThrows(CatalogException.class,
                () -> catalog.advanceCutline(table, new TierKey(1500), new LakeSnapshotId(6)));
        assertThrows(CatalogException.class,
                () -> catalog.advanceCutline(table, new TierKey(2500), new LakeSnapshotId(4)));
        assertEquals(new Cutline(new TierKey(2000), new LakeSnapshotId(5)), catalog.readCutline(table));
    }

    @Test
    void compactionAdvancesSnapshotClearsFoldedRowsAndKeepsT() {
        exec("INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload) VALUES "
                + "(" + table.oid() + ", '7', 0, 500, 3, '{\"id\":7}'), "
                + "(" + table.oid() + ", '8', 1, 600, 9, NULL)");

        // pk=8 was re-corrected after the fold (version 9 > folded 4): it must survive.
        catalog.publishCompaction(table, new LakeSnapshotId(9),
                new TestBatch(table, 2, List.of(new DeltaBatch.Key("7", 3), new DeltaBatch.Key("8", 4))),
                java.util.Map.of("metadata_location", "/wh/events/metadata/00005-c.metadata.json"));

        Cutline c = catalog.readCutline(table);
        assertEquals(new TierKey(1000), c.t());
        assertEquals(new LakeSnapshotId(9), c.snapshot());
        assertTrue(catalog.get(table).orElseThrow().lakeProps().contains("00005-c.metadata.json"));

        try (Connection conn = dataSource.getConnection(); Statement s = conn.createStatement()) {
            var rs = s.executeQuery("SELECT pk FROM modak.delta ORDER BY pk");
            assertTrue(rs.next());
            assertEquals("8", rs.getString(1));
            assertFalse(rs.next());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void compactionPublishIsBlockedByAnActivePin() {
        addPin(new TierKey(1000), new LakeSnapshotId(1));
        assertThrows(CatalogException.class, () -> catalog.publishCompaction(
                table, new LakeSnapshotId(9), new TestBatch(table, 0, List.of()),
                java.util.Map.of()));
        assertEquals(new LakeSnapshotId(1), catalog.readCutline(table).snapshot());
    }

    @Test
    void horizonIsOldestPinElseCutline() {
        assertEquals(new Cutline(new TierKey(1000), new LakeSnapshotId(1)), catalog.readHorizon(table));

        catalog.advanceCutline(table, new TierKey(2000), new LakeSnapshotId(10));
        addPin(new TierKey(1200), new LakeSnapshotId(4));
        addPin(new TierKey(1600), new LakeSnapshotId(7));
        assertEquals(new Cutline(new TierKey(1200), new LakeSnapshotId(4)), catalog.readHorizon(table));
    }

    @Test
    void expiredPinsDoNotHoldTheHorizonOrBlockCompaction() {
        catalog.advanceCutline(table, new TierKey(2000), new LakeSnapshotId(10));
        exec("INSERT INTO modak.read_pins (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) "
                + "VALUES (" + table.oid() + ", 1, 100, now() - interval '1 minute')");

        assertEquals(new Cutline(new TierKey(2000), new LakeSnapshotId(10)),
                catalog.readHorizon(table), "a crashed reader's expired pin is ignored");
        catalog.publishCompaction(table, new LakeSnapshotId(11),
                new TestBatch(table, 0, List.of()), java.util.Map.of());
        assertEquals(new LakeSnapshotId(11), catalog.readCutline(table).snapshot());
    }

    @Test
    void advanceWithPropsPatchMergesLakePropsAtomically() {
        catalog.advanceCutline(table, new TierKey(2000), new LakeSnapshotId(5),
                java.util.Map.of("metadata_location", "/wh/events/metadata/00003-x.metadata.json"));

        assertEquals(new Cutline(new TierKey(2000), new LakeSnapshotId(5)), catalog.readCutline(table));
        String props = catalog.get(table).orElseThrow().lakeProps();
        assertTrue(props.contains("00003-x.metadata.json"), props);
        assertTrue(props.contains("\"catalog\""), "pre-existing keys survive the merge: " + props);

        assertThrows(CatalogException.class,
                () -> catalog.advanceCutline(table, new TierKey(1000), new LakeSnapshotId(9),
                        java.util.Map.of("metadata_location", "/wh/should-not-appear.json")));
        assertFalse(catalog.get(table).orElseThrow().lakeProps().contains("should-not-appear"));
    }

    @Test
    void opLogTracksPhaseAndFindsIncompleteWork() {
        var opId = java.util.UUID.randomUUID();
        catalog.logOpPhase(opId, table, TieringOp.KIND_TIERING, TieringOp.PHASE_FLUSHING,
                null, "{\"partitions\":[\"p0\"]}");
        catalog.logOpPhase(opId, table, TieringOp.KIND_TIERING, TieringOp.PHASE_COMMITTED,
                new LakeSnapshotId(12), null);

        List<TieringOp> pending = catalog.findIncompleteOps(table, TieringOp.KIND_TIERING);
        assertEquals(1, pending.size());
        TieringOp op = pending.get(0);
        assertEquals(opId, op.opId());
        assertEquals(TieringOp.PHASE_COMMITTED, op.phase());
        assertEquals(new LakeSnapshotId(12), op.snapshot().orElseThrow());
        assertTrue(op.detailsJson().contains("p0"), "details survive phase upserts");

        catalog.logOpPhase(opId, table, TieringOp.KIND_TIERING, TieringOp.PHASE_ADVANCED, null, null);
        assertTrue(catalog.findIncompleteOps(table, TieringOp.KIND_TIERING).isEmpty());
    }

    @Test
    void partitionFollowsLegalLifecycleOnly() {
        var p = new PartitionId(table, "2026-07-01T00");
        catalog.upsertPartition(p, new PartitionBounds(new TierKey(0), new TierKey(1000)), PartitionState.HOT);

        catalog.transition(p, PartitionState.HOT, PartitionState.SEALING);
        catalog.transition(p, PartitionState.SEALING, PartitionState.TIERING);

        assertThrows(IllegalTransitionException.class,
                () -> catalog.transition(p, PartitionState.TIERING, PartitionState.DROPPED));
        assertThrows(CatalogException.class,
                () -> catalog.transition(p, PartitionState.HOT, PartitionState.SEALING));

        catalog.transition(p, PartitionState.TIERING, PartitionState.TIERED);
        catalog.transition(p, PartitionState.TIERED, PartitionState.DROPPED);

        List<PartitionInfo> parts = catalog.listPartitions(table);
        assertEquals(1, parts.size());
        assertEquals(PartitionState.DROPPED, parts.get(0).state());
    }

    @Test
    void retentionPublishRaisesLinePurgesDeltaAndDropsExpiredPartitionRows() {
        exec("INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload) VALUES "
                + "(" + table.oid() + ", '1', 0, 100, 1, '{\"id\":1}'), "
                + "(" + table.oid() + ", '2', 0, 700, 2, '{\"id\":2}')");
        catalog.upsertPartition(new PartitionId(table, "p0"),
                new PartitionBounds(new TierKey(0), new TierKey(500)), PartitionState.DROPPED);
        catalog.upsertPartition(new PartitionId(table, "p1"),
                new PartitionBounds(new TierKey(500), new TierKey(1000)), PartitionState.DROPPED);

        catalog.publishRetention(table, new LakeSnapshotId(6), new TierKey(500),
                java.util.Map.of("metadata_location", "/wh/events/metadata/00006-r.metadata.json"));

        assertEquals(new TierKey(500), catalog.readRetentionLine(table).orElseThrow());
        assertEquals(new LakeSnapshotId(6), catalog.readCutline(table).snapshot());
        assertTrue(catalog.get(table).orElseThrow().lakeProps().contains("00006-r.metadata.json"));
        assertEquals(1, catalog.listPartitions(table).size(), "p0 below R is gone");
        try (Connection conn = dataSource.getConnection(); Statement s = conn.createStatement()) {
            var rs = s.executeQuery("SELECT pk FROM modak.delta ORDER BY pk");
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1), "only the above-R correction survives");
            assertFalse(rs.next());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Without a lake commit the line still moves, S stays.
        catalog.publishRetention(table, null, new TierKey(900), java.util.Map.of());
        assertEquals(new TierKey(900), catalog.readRetentionLine(table).orElseThrow());
        assertEquals(new LakeSnapshotId(6), catalog.readCutline(table).snapshot());

        assertThrows(CatalogException.class,
                () -> catalog.publishRetention(table, null, new TierKey(400), java.util.Map.of()));
    }

    @Test
    void retentionPublishIsBlockedByAnActivePin() {
        addPin(new TierKey(1000), new LakeSnapshotId(1));
        assertThrows(CatalogException.class, () -> catalog.publishRetention(
                table, new LakeSnapshotId(9), new TierKey(500), java.util.Map.of()));
        assertTrue(catalog.readRetentionLine(table).isEmpty());
    }

    private void addPin(TierKey t, LakeSnapshotId snapshot) {
        exec("INSERT INTO modak.read_pins (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) "
                + "VALUES (" + table.oid() + ", " + snapshot.id() + ", " + t.value()
                + ", now() + interval '1 hour')");
    }

    private static void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
