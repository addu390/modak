package io.tierdb.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.InMemoryCatalog;
import io.tierdb.catalog.MaintenancePolicy;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.TableMode;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.tiering.PartitionPremake;
import io.tierdb.tiering.PartitionSync;
import io.tierdb.common.TierKeyType;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Premake keeps empty partition widths ahead of the write frontier so inserts
 * never land without a partition, and flags rows already past the range grid.
 */
class PartitionPremakeTest {

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;

    @BeforeAll
    static void setUp() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void keepsHeadroomAboveTheHighWaterAndSyncRegistersTheNewPartitionsHot() {
        exec("CREATE TABLE public.metrics (id bigint, ts bigint) PARTITION BY RANGE (ts)");
        exec("CREATE TABLE public.metrics_p0 PARTITION OF public.metrics FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.metrics_p100 PARTITION OF public.metrics FOR VALUES FROM (100) TO (200)");
        exec("INSERT INTO public.metrics VALUES (1, 150)");
        RegisteredTable table = registered("metrics", 101L);

        var result = new PartitionPremake(dataSource, 2).premake(table).orElseThrow();

        assertEquals(2, result.created(), "150 + 2*100 headroom needs bounds up to 350");
        assertFalse(result.outsideGrid());
        assertNotNull(queryOne("SELECT to_regclass('public.metrics_p200')::text"));
        assertNotNull(queryOne("SELECT to_regclass('public.metrics_p300')::text"));
        assertNull(queryOne("SELECT to_regclass('public.metrics_p400')::text"));

        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.register(new TableRegistration(101L, "public", "metrics",
                List.of("id"), "ts", "{\"unit\":\"range\",\"partition_width\":100}",
                "iceberg", "/wh/metrics"));
        new PartitionSync(dataSource, catalog).sync(table);
        assertTrue(catalog.listPartitions(new TableId(101L)).stream()
                .allMatch(p -> p.state() == PartitionState.HOT));
        assertEquals(4, catalog.listPartitions(new TableId(101L)).size());

        var again = new PartitionPremake(dataSource, 2).premake(table).orElseThrow();
        assertEquals(0, again.created());
    }

    @Test
    void emptyTablesAndUnpartitionedTablesAreLeftAlone() {
        exec("CREATE TABLE public.empty_t (id bigint, ts bigint) PARTITION BY RANGE (ts)");
        exec("CREATE TABLE public.empty_t_p0 PARTITION OF public.empty_t FOR VALUES FROM (0) TO (100)");
        assertEquals(0, new PartitionPremake(dataSource, 2)
                .premake(registered("empty_t", 102L)).orElseThrow().created());

        exec("CREATE TABLE public.plain_t (id bigint, ts bigint)");
        assertTrue(new PartitionPremake(dataSource, 2)
                .premake(registered("plain_t", 103L)).isEmpty(), "no range partitions");
    }

    @Test
    void rowsPastTheTopBoundAreFlaggedNotPapered() {
        exec("CREATE TABLE public.spiky (id bigint, ts bigint) PARTITION BY RANGE (ts)");
        exec("CREATE TABLE public.spiky_p0 PARTITION OF public.spiky FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.spiky_default PARTITION OF public.spiky DEFAULT");
        exec("INSERT INTO public.spiky VALUES (1, 500)");

        var result = new PartitionPremake(dataSource, 2)
                .premake(registered("spiky", 104L)).orElseThrow();

        assertTrue(result.outsideGrid());
        assertEquals(0, result.created());
    }

    private static RegisteredTable registered(String table, long oid) {
        return new RegisteredTable(new TableId(oid), "public", table, List.of("id"), "ts",
                "{\"unit\":\"range\",\"partition_width\":100}", "iceberg", "/wh/" + table,
                "default", TableMode.TIERED, null, null, Optional.empty(), Optional.empty(),
                false, MaintenancePolicy.NONE, TierKeyType.BIGINT);
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
