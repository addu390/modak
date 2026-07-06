package io.tierdb.catalog;

import io.tierdb.common.OpPhase;
import io.tierdb.common.OpKind;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.common.Cutline;
import io.tierdb.common.DeltaBatch;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.Lsn;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
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
        exec("TRUNCATE tierdb.tables CASCADE");
        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                42L, "public", "events",
                List.of("id"), "event_time",
                "{\"unit\":\"hour\"}", "iceberg", "warehouse.public.events"));
        catalog.initCutline(table, new TierKey(1000), new LakeSnapshotId(1),
                "{\"catalog\":\"rest\"}");
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
                "{}", "iceberg", "warehouse.public.vehicles",
                TableMode.MIRRORED, "tierdb_pub_vehicles", "tierdb_slot_vehicles",
                java.util.Optional.of(3_600_000_000L), java.util.Optional.empty()));

        RegisteredTable t = catalog.get(mirrored).orElseThrow();
        assertEquals(TableMode.MIRRORED, t.mode());
        assertEquals("tierdb_pub_vehicles", t.publicationName());
        assertEquals("tierdb_slot_vehicles", t.slotName());
        assertEquals(3_600_000_000L, t.heapRetentionLag().orElseThrow());
        assertTrue(t.dropsHeapPartitions());
    }

    @Test
    void keepHeapRegistrationRoundTripsAndNeverDropsPartitions() {
        TableId kh = catalog.register(new TableRegistration(
                88L, "public", "sensor_readings", List.of("id"), "reading_time",
                "{}", "iceberg", "warehouse.public.sensor_readings",
                TableMode.TIERED, null, null,
                java.util.Optional.empty(), java.util.Optional.empty(), true));

        RegisteredTable t = catalog.get(kh).orElseThrow();
        assertTrue(t.keepHeap());
        assertFalse(t.dropsHeapPartitions());
        assertFalse(catalog.get(table).orElseThrow().keepHeap(), "default is false");
    }

    @Test
    void keepHeapIsTieredOnlyAndExcludesLakeRetention() {
        assertThrows(IllegalArgumentException.class, () -> new TableRegistration(
                89L, "public", "x", List.of("id"), "ts", "{}", "iceberg", "w.x",
                TableMode.MIRRORED, "pub", "slot",
                java.util.Optional.empty(), java.util.Optional.empty(), true));
        assertThrows(IllegalArgumentException.class, () -> new TableRegistration(
                90L, "public", "y", List.of("id"), "ts", "{}", "iceberg", "w.y",
                TableMode.TIERED, null, null,
                java.util.Optional.empty(), java.util.Optional.of(100L), true));
    }

    @Test
    void mirrorFrontierSeedsFromNullThenAdvancesMonotonically() {
        catalog.advanceMirrorFrontier(table, new Lsn(100), new LakeSnapshotId(2),
                java.util.Map.of("metadata_location", "/wh/events/metadata/00002-m.metadata.json"));
        assertEquals(new Lsn(100), catalog.readMirrorFrontier(table).orElseThrow());
        assertEquals(new LakeSnapshotId(2), catalog.readCutline(table).snapshot());
        assertTrue(catalog.readLakeProps(table).orElseThrow().contains("00002-m.metadata.json"));

        assertThrows(CatalogException.class,
                () -> catalog.advanceMirrorFrontier(table, new Lsn(50), new LakeSnapshotId(3),
                        java.util.Map.of("metadata_location", "/wh/should-not-appear.json")));
        assertThrows(CatalogException.class,
                () -> catalog.advanceMirrorFrontier(table, new Lsn(200), new LakeSnapshotId(1),
                        java.util.Map.of()));
        assertEquals(new Lsn(100), catalog.readMirrorFrontier(table).orElseThrow());
        assertFalse(catalog.readLakeProps(table).orElseThrow().contains("should-not-appear"));
    }

    @Test
    void lookupOfUnknownTableIsEmpty() {
        assertFalse(catalog.lookup("public", "nope").isPresent());
    }

    @Test
    void duplicateRegistrationRejectedByUniqueConstraint() {
        assertThrows(CatalogException.class, () -> catalog.register(new TableRegistration(
                99L, "public", "events", List.of("id"), "event_time",
                "{}", "iceberg", "warehouse.public.events")));
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
        exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES "
                + "(" + table.oid() + ", '7', 0, 500, 3, '{\"id\":7}'), "
                + "(" + table.oid() + ", '8', 1, 600, 9, NULL)");

        catalog.publishCompaction(table, new LakeSnapshotId(9),
                new TestBatch(table, 2, List.of(new DeltaBatch.Key("7", 3), new DeltaBatch.Key("8", 4))),
                java.util.Map.of("metadata_location", "/wh/events/metadata/00005-c.metadata.json"));

        Cutline c = catalog.readCutline(table);
        assertEquals(new TierKey(1000), c.t());
        assertEquals(new LakeSnapshotId(9), c.snapshot());
        assertTrue(catalog.readLakeProps(table).orElseThrow().contains("00005-c.metadata.json"));

        try (Connection conn = dataSource.getConnection(); Statement s = conn.createStatement()) {
            var rs = s.executeQuery("SELECT pk FROM tierdb.delta ORDER BY pk");
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
        exec("INSERT INTO tierdb.read_pins (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) "
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
        String props = catalog.readLakeProps(table).orElseThrow();
        assertTrue(props.contains("00003-x.metadata.json"), props);
        assertTrue(props.contains("\"catalog\""), "pre-existing keys survive the merge: " + props);

        assertThrows(CatalogException.class,
                () -> catalog.advanceCutline(table, new TierKey(1000), new LakeSnapshotId(9),
                        java.util.Map.of("metadata_location", "/wh/should-not-appear.json")));
        assertFalse(catalog.readLakeProps(table).orElseThrow().contains("should-not-appear"));
    }

    @Test
    void opLogTracksPhaseAndFindsIncompleteWork() {
        var opId = java.util.UUID.randomUUID();
        catalog.logOpPhase(opId, table, OpKind.TIERING, OpPhase.FLUSHING,
                null, "{\"partitions\":[\"p0\"]}");
        catalog.logOpPhase(opId, table, OpKind.TIERING, OpPhase.COMMITTED,
                new LakeSnapshotId(12), null);

        List<TieringOp> pending = catalog.findIncompleteOps(table, OpKind.TIERING);
        assertEquals(1, pending.size());
        TieringOp op = pending.get(0);
        assertEquals(opId, op.opId());
        assertEquals(OpPhase.COMMITTED, op.phase());
        assertEquals(new LakeSnapshotId(12), op.snapshot().orElseThrow());
        assertTrue(op.detailsJson().contains("p0"), "details survive phase upserts");

        catalog.logOpPhase(opId, table, OpKind.TIERING, OpPhase.ADVANCED, null, null);
        assertTrue(catalog.findIncompleteOps(table, OpKind.TIERING).isEmpty());
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
        exec("INSERT INTO tierdb.delta (table_id, pk, op, tier_key, version, payload) VALUES "
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
        assertTrue(catalog.readLakeProps(table).orElseThrow().contains("00006-r.metadata.json"));
        assertEquals(1, catalog.listPartitions(table).size(), "p0 below R is gone");
        try (Connection conn = dataSource.getConnection(); Statement s = conn.createStatement()) {
            var rs = s.executeQuery("SELECT pk FROM tierdb.delta ORDER BY pk");
            assertTrue(rs.next());
            assertEquals("2", rs.getString(1), "only the above-R correction survives");
            assertFalse(rs.next());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        catalog.publishRetention(table, null, new TierKey(900), java.util.Map.of());
        assertEquals(new TierKey(900), catalog.readRetentionLine(table).orElseThrow());
        assertEquals(new LakeSnapshotId(6), catalog.readCutline(table).snapshot());

        assertThrows(CatalogException.class,
                () -> catalog.publishRetention(table, null, new TierKey(400), java.util.Map.of()));
    }

    @Test
    void loadLabelInsertsOnceAndReplaysAreVisible() {
        assertTrue(catalog.beginLoad(table, "batch-1", LoadState.COMMITTED,
                null, "{\"rows\":3}"));
        assertFalse(catalog.beginLoad(table, "batch-1", LoadState.COMMITTED,
                null, "{\"rows\":99}"), "a replayed label never re-applies");

        LoadLabel l = catalog.lookupLoad(table, "batch-1").orElseThrow();
        assertEquals(LoadState.COMMITTED, l.state());
        assertTrue(l.resultJson().contains("\"rows\": 3"), "the first result wins: " + l);
        assertFalse(catalog.lookupLoad(table, "never-seen").isPresent());
    }

    @Test
    void stagedLoadsListOldestFirstAndFinishLoadFlipsThemWithS() {
        catalog.beginLoad(table, "older", LoadState.STAGED,
                "[\"/wh/s/a.parquet\"]", "{\"rows\":10}");
        catalog.beginLoad(table, "newer", LoadState.STAGED,
                "[\"/wh/s/b.parquet\"]", "{\"rows\":20}");
        catalog.beginLoad(table, "done", LoadState.COMMITTED, null, "{\"rows\":1}");

        List<LoadLabel> staged = catalog.stagedLoads(table);
        assertEquals(List.of("older", "newer"),
                staged.stream().map(LoadLabel::label).toList());
        assertTrue(staged.get(0).stagedFilesJson().contains("a.parquet"));

        catalog.finishLoad(table, List.of("older", "newer"), new LakeSnapshotId(7),
                java.util.Map.of("metadata_location", "/wh/events/metadata/00007-l.metadata.json"));

        assertTrue(catalog.stagedLoads(table).isEmpty());
        assertEquals(LoadState.COMMITTED,
                catalog.lookupLoad(table, "older").orElseThrow().state());
        assertEquals(new LakeSnapshotId(7), catalog.readCutline(table).snapshot());
        assertEquals(new TierKey(1000), catalog.readCutline(table).t(), "T is untouched");
        assertTrue(catalog.readLakeProps(table).orElseThrow().contains("00007-l.metadata.json"));
    }

    @Test
    void finishLoadNeverRegressesTheSnapshot() {
        catalog.advanceCutline(table, new TierKey(1000), new LakeSnapshotId(10));
        catalog.beginLoad(table, "late", LoadState.STAGED, "[]", null);

        catalog.finishLoad(table, List.of("late"), new LakeSnapshotId(5), java.util.Map.of());

        assertEquals(new LakeSnapshotId(10), catalog.readCutline(table).snapshot());
        assertEquals(LoadState.COMMITTED,
                catalog.lookupLoad(table, "late").orElseThrow().state());
    }

    @Test
    void loadLabelsCascadeWithTheTable() {
        catalog.beginLoad(table, "batch-1", LoadState.STAGED, "[]", null);
        catalog.unregister(table);
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            var rs = s.executeQuery("SELECT count(*) FROM tierdb.load_labels");
            assertTrue(rs.next());
            assertEquals(0, rs.getLong(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void maintenanceRequestUpsertsOnceAndIsClaimedByExactlyOneConsumer() {
        assertFalse(catalog.consumeMaintenanceRequest(table), "nothing pending yet");

        catalog.requestMaintenance(table, "cli");
        catalog.requestMaintenance(table, "console");

        assertTrue(catalog.consumeMaintenanceRequest(table), "first claim wins");
        assertFalse(catalog.consumeMaintenanceRequest(table), "the request is consumed");
    }

    @Test
    void retentionPublishIsBlockedByAnActivePin() {
        addPin(new TierKey(1000), new LakeSnapshotId(1));
        assertThrows(CatalogException.class, () -> catalog.publishRetention(
                table, new LakeSnapshotId(9), new TierKey(500), java.util.Map.of()));
        assertTrue(catalog.readRetentionLine(table).isEmpty());
    }

    private void addPin(TierKey t, LakeSnapshotId snapshot) {
        exec("INSERT INTO tierdb.read_pins (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at) "
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
