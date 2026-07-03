package io.modak.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.modak.catalog.CatalogSchema;
import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.RowBatchData.Column;
import io.modak.common.TableId;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeSnapshotReader;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTable;
import io.modak.lake.LakeTieringFactory;
import io.modak.lake.MaintenanceConfig;
import io.modak.lake.MaintenanceResult;
import io.modak.lake.MergeWriter;
import io.modak.lake.iceberg.IcebergLakeStoragePlugin;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The chunked initial copy. A crash mid-copy leaves a resumable journal, and a
 * re-run of register completes the copy from the last chunk. Rows written while
 * the copy was down arrive via the stream from the original consistent point.
 */
class ResumableCopyEndToEndTest {

    @TempDir
    static Path warehouse;

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static WorkerConfig config;
    private static LakeStorage realLake;

    @BeforeAll
    static void setUpWorld() throws Exception {
        postgres = EmbeddedPostgres.builder()
                .setServerConfig("wal_level", "logical")
                .start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);

        exec("CREATE TABLE public.readings (id bigint PRIMARY KEY, val text, ts bigint NOT NULL)");
        exec("INSERT INTO public.readings SELECT g, 'v' || g, g * 10 FROM generate_series(1, 5) g");

        config = new WorkerConfig(
                postgres.getJdbcUrl("postgres", "postgres"), "postgres", "",
                warehouse.toString(), Map.of(),
                10, 0, 0, 1000, 500, 200, 1);
        catalog = new JdbcCatalog(config.dataSource());
        realLake = new IcebergLakeStoragePlugin().create(config.lakeConfig());
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void aCrashedCopyResumesFromTheJournalWithNoGapsAndNoDuplicates() throws Exception {
        String[] args = {
            "--table", "public.readings", "--pk", "id", "--tier-key", "ts",
            "--mode", "mirrored", "--chunk-rows", "2",
        };

        // "Crash" after the first chunk committed and journaled.
        assertThrows(Exception.class,
                () -> TableRegistrar.run(config, args, new CrashingLake(realLake, 1)));

        TableId table = new TableId(Long.parseLong(
                queryOne("SELECT 'public.readings'::regclass::oid::bigint::text")));
        assertEquals("1", queryOne("SELECT chunks_done::text FROM modak.copy_progress "
                + "WHERE table_id = " + table.oid()), "one chunk journaled before the crash");
        assertTrue(catalog.readMirrorFrontier(table).isEmpty(),
                "no frontier until the copy lands, the daemon skips this table");

        // Writes while down: one below the copied range (stream), one above (resumed chunks).
        exec("INSERT INTO public.readings VALUES (0, 'v0', 5)");
        exec("INSERT INTO public.readings VALUES (9, 'v9', 90)");

        TableRegistrar.run(config, args, realLake);

        assertEquals("0", queryOne("SELECT count(*)::text FROM modak.copy_progress "
                + "WHERE table_id = " + table.oid()), "journal cleared on completion");
        Optional<Lsn> frontier = catalog.readMirrorFrontier(table);
        assertTrue(frontier.isPresent(), "frontier seeded at the original consistent point");

        assertEquals(
                List.of("1|v1|10", "2|v2|20", "3|v3|30", "4|v4|40", "5|v5|50", "9|v9|90"),
                lakeRows(),
                "resume finished the copy from the last chunk without duplicating chunk 1");

        // The below-range row arrives via the stream from the consistent point.
        RegisteredTable meta = catalog.get(table).orElseThrow();
        MirrorWorker worker = new MirrorWorker(catalog, realLake, meta,
                config.pgUrl(), config.pgUser(), config.pgPassword(),
                config.mirrorBatchRows(), config.mirrorFlushMillis());
        Thread pump = new Thread(worker, "resume-test-pump");
        pump.setDaemon(true);
        pump.start();
        try {
            awaitFrontierPast(table, currentWalLsn());
            assertEquals(
                    List.of("0|v0|5", "1|v1|10", "2|v2|20", "3|v3|30", "4|v4|40",
                            "5|v5|50", "9|v9|90"),
                    lakeRows(),
                    "stream replay healed the row the crashed copy raced with");
        } finally {
            worker.stop();
            pump.join(15_000);
        }
    }

    /** Delegates to the real lake but fails the Nth+1 merge apply. */
    private static final class CrashingLake implements LakeStorage {
        private final LakeStorage delegate;
        private int remaining;

        CrashingLake(LakeStorage delegate, int successfulApplies) {
            this.delegate = delegate;
            this.remaining = successfulApplies;
        }

        @Override
        public String tableRef(String schema, String table) {
            return delegate.tableRef(schema, table);
        }

        @Override
        public String createTableIfAbsent(String ref, List<Column> columns,
                Set<String> requiredCols, String tierKeyCol, long partitionWidth) {
            return delegate.createTableIfAbsent(ref, columns, requiredCols, tierKeyCol,
                    partitionWidth);
        }

        @Override
        public void dropTable(String ref) {
            delegate.dropTable(ref);
        }

        @Override
        public LakeTieringFactory<?, ?> tieringFactory() {
            return delegate.tieringFactory();
        }

        @Override
        public LakeSnapshotReader snapshotReader() {
            return delegate.snapshotReader();
        }

        @Override
        public LakeTable table(CommitterInitContext ctx, ColdTableSpec spec) {
            return new CrashingLakeTable(delegate.table(ctx, spec));
        }

        private final class CrashingLakeTable implements LakeTable {
            private final LakeTable inner;

            CrashingLakeTable(LakeTable inner) {
                this.inner = inner;
            }

            @Override
            public MergeWriter mergeWriter() {
                if (remaining-- <= 0) {
                    throw new IllegalStateException("injected crash between copy chunks");
                }
                return inner.mergeWriter();
            }

            @Override
            public void evolveSchema(List<Column> addColumns) {
                inner.evolveSchema(addColumns);
            }

            @Override
            public MaintenanceResult maintain(MaintenanceConfig config,
                    LakeSnapshotId oldestPinnedSnapshot, Map<String, String> snapshotProps) {
                return inner.maintain(config, oldestPinnedSnapshot, snapshotProps);
            }

            @Override
            public io.modak.lake.LakeCommitResult expireBelow(long boundary,
                    Map<String, String> snapshotProps) {
                return inner.expireBelow(boundary, snapshotProps);
            }

            @Override
            public io.modak.lake.LakeCommitResult ingest(java.util.List<String> files,
                    io.modak.lake.TierKeyWindow window, Map<String, String> snapshotProps) {
                return inner.ingest(files, window, snapshotProps);
            }

            @Override
            public java.util.List<String> stageRows(java.util.List<String> columns,
                    Iterable<Object[]> rows) {
                return inner.stageRows(columns, rows);
            }
        }
    }

    private static long currentWalLsn() {
        return Long.parseLong(
                queryOne("SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint::text"));
    }

    private static void awaitFrontierPast(TableId table, long targetLsn)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            Optional<Lsn> frontier = catalog.readMirrorFrontier(table);
            if (frontier.isPresent() && frontier.get().value() >= targetLsn) {
                return;
            }
            Thread.sleep(100);
        }
        fail("mirror frontier never reached " + targetLsn);
    }

    private static List<String> lakeRows() throws IOException {
        Table lakeTable = new HadoopTables(new Configuration())
                .load(warehouse.toString().replaceAll("/+$", "") + "/public.readings");
        List<String> rows = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(lakeTable).build()) {
            for (Record r : records) {
                rows.add(r.getField("id") + "|" + r.getField("val") + "|" + r.getField("ts"));
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String queryOne(String sql) {
        try (Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
