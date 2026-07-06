package io.tierdb.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.tierdb.catalog.CatalogSchema;
import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.PartitionInfo;
import io.tierdb.catalog.TableRegistration;
import io.tierdb.common.LakeSnapshotId;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TableId;
import io.tierdb.common.TierKey;
import io.tierdb.tiering.policy.LagBasedTieringPolicy;
import io.tierdb.tiering.PartitionSync;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The daemon's discovery pieces against a real Postgres: {@link PartitionSync} */
class PartitionLifecycleTest {

    private static EmbeddedPostgres postgres;
    private static DataSource dataSource;
    private static JdbcCatalog catalog;
    private static TableId table;

    @BeforeAll
    static void setUp() throws IOException {
        postgres = EmbeddedPostgres.builder().start();
        dataSource = postgres.getPostgresDatabase();
        CatalogSchema.apply(dataSource);

        exec("""
                CREATE TABLE public.events (
                    id bigint NOT NULL, event_time bigint NOT NULL, val text
                ) PARTITION BY RANGE (event_time)
                """);
        exec("CREATE TABLE public.events_p0 PARTITION OF public.events FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.events_p1 PARTITION OF public.events FOR VALUES FROM (100) TO (200)");
        exec("CREATE TABLE public.events_p2 PARTITION OF public.events FOR VALUES FROM (200) TO (300)");

        catalog = new JdbcCatalog(dataSource);
        table = catalog.register(new TableRegistration(
                relOid("public.events"), "public", "events", List.of("id"), "event_time",
                "{\"unit\":\"range\"}", "iceberg", "/wh/events"));
        catalog.initCutline(table, new TierKey(0), new LakeSnapshotId(0));
    }

    @AfterAll
    static void tearDown() throws IOException {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void syncDiscoversPartitionsAndPreservesManagedState() {
        PartitionSync sync = new PartitionSync(dataSource, catalog);

        assertEquals(3, sync.sync(catalog.get(table).orElseThrow()));
        PartitionInfo p0 = partition("events_p0");
        assertEquals(0, p0.bounds().lo().value());
        assertEquals(100, p0.bounds().hi().value());
        assertEquals(PartitionState.HOT, p0.state());

        catalog.transition(p0.id(), PartitionState.HOT, PartitionState.SEALING);
        exec("CREATE TABLE public.events_p3 PARTITION OF public.events FOR VALUES FROM (300) TO (400)");
        assertEquals(1, sync.sync(catalog.get(table).orElseThrow()));
        assertEquals(PartitionState.SEALING, partition("events_p0").state());
        assertEquals(PartitionState.HOT, partition("events_p3").state());

        catalog.transition(p0.id(), PartitionState.SEALING, PartitionState.TIERING);
        catalog.transition(p0.id(), PartitionState.TIERING, PartitionState.TIERED);
    }

    @Test
    void lagPolicySelectsTheContiguousRunBehindTheHighWater() {
        exec("""
                CREATE TABLE public.metrics (
                    id bigint NOT NULL, event_time bigint NOT NULL, val text
                ) PARTITION BY RANGE (event_time)
                """);
        exec("CREATE TABLE public.metrics_p0 PARTITION OF public.metrics FOR VALUES FROM (0) TO (100)");
        exec("CREATE TABLE public.metrics_p1 PARTITION OF public.metrics FOR VALUES FROM (100) TO (200)");
        exec("CREATE TABLE public.metrics_p2 PARTITION OF public.metrics FOR VALUES FROM (200) TO (300)");
        TableId metrics = catalog.register(new TableRegistration(
                relOid("public.metrics"), "public", "metrics", List.of("id"), "event_time",
                "{\"unit\":\"range\"}", "iceberg", "/wh/metrics"));
        catalog.initCutline(metrics, new TierKey(0), new LakeSnapshotId(0));
        new PartitionSync(dataSource, catalog).sync(catalog.get(metrics).orElseThrow());

        exec("INSERT INTO public.metrics VALUES (1, 10, 'a'), (2, 150, 'b'), (3, 250, 'hot')");

        assertEquals(List.of(new PartitionId(metrics, "metrics_p0")),
                new LagBasedTieringPolicy(dataSource, catalog, 100)
                        .selectForTiering(metrics, Instant.now()));

        assertEquals(
                List.of(new PartitionId(metrics, "metrics_p0"), new PartitionId(metrics, "metrics_p1")),
                new LagBasedTieringPolicy(dataSource, catalog, 0)
                        .selectForTiering(metrics, Instant.now()));

        exec("TRUNCATE public.metrics");
        assertTrue(new LagBasedTieringPolicy(dataSource, catalog, 0)
                .selectForTiering(metrics, Instant.now()).isEmpty());
    }

    private static PartitionInfo partition(String name) {
        return catalog.listPartitions(table).stream()
                .filter(p -> p.id().id().equals(name)).findFirst().orElseThrow();
    }

    private static long relOid(String qualified) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT '" + qualified + "'::regclass::oid::bigint")) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void exec(String sql) {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
