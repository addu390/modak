package io.modak.worker;

import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.catalog.TableMode;
import io.modak.cdc.CdcException;
import io.modak.cdc.ReplicationSource;
import io.modak.lake.LakeStorage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * The offboarding command ({@code modak-worker unregister}). Drops the CDC
 * plumbing, removes the catalog row (cascading cutline/partitions/delta/pins),
 * and, only with {@code --drop-lake}, purges the cold table. Also cleans up
 * the plumbing a crashed registration left behind without a catalog row.
 */
final class TableUnregistrar {

    private TableUnregistrar() {}

    static void run(WorkerConfig config, String[] args) throws Exception {
        String qualified = new Args(args).required("--table");
        boolean dropLake = hasFlag(args, "--drop-lake");
        String[] parts = qualified.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("--table must be schema-qualified: " + qualified);
        }
        String schema = parts[0];
        String table = parts[1];

        DataSource ds = config.dataSource();
        JdbcCatalog catalog = new JdbcCatalog(ds);
        Optional<RegisteredTable> registered = catalog.lookup(schema, table);

        String publication = registered.map(RegisteredTable::publicationName)
                .orElse(TableRegistrar.replicationName("modak_pub", schema, table));
        String slot = registered.map(RegisteredTable::slotName)
                .orElse(TableRegistrar.replicationName("modak_slot", schema, table));

        registered.ifPresent(meta -> {
            catalog.unregister(meta.id());
            Log.info("unregistered %s (table_id=%d)", qualified, meta.id().oid());
        });

        try (Connection admin = ds.getConnection()) {
            evictAndDropSlot(admin, slot);
            ReplicationSource.dropPublication(admin, publication);
            if (registered.map(t -> t.mode() == TableMode.MIRRORED).orElse(true)) {
                resetReplicaIdentity(admin, qualified);
            }
        }

        if (registered.isEmpty()) {
            Log.info("%s is not registered; cleaned up any leftover slot/publication", qualified);
            return;
        }

        RegisteredTable meta = registered.get();

        if (dropLake) {
            LakeStorage lake = LakePlugins.load(meta.lakeFormat(), config.lakeConfig());
            lake.dropTable(meta.lakeTableRef());
            Log.info("dropped lake table %s (data files purged)", meta.lakeTableRef());
        } else if (meta.mode() == TableMode.TIERED) {
            Log.warn("lake table %s kept, it holds the ONLY copy of tiered rows whose "
                    + "heap partitions were reclaimed, re-run with --drop-lake to purge it",
                    meta.lakeTableRef());
        } else {
            Log.info("lake table %s kept, re-run with --drop-lake to purge it",
                    meta.lakeTableRef());
        }
    }

    private static void evictAndDropSlot(Connection admin, String slot) throws Exception {
        Exception last = null;
        for (int i = 0; i < 30; i++) {
            try (PreparedStatement ps = admin.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                            + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
                ps.setString(1, slot);
                ps.execute();
            }
            try {
                ReplicationSource.dropSlot(admin, slot);
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(500);
            }
        }
        throw new CdcException("could not drop replication slot " + slot
                + ", is another consumer reconnecting to it?", last);
    }

    // The registered heap table may already be dropped, so identity reset is best effort.
    private static void resetReplicaIdentity(Connection admin, String qualified) {
        try (Statement s = admin.createStatement()) {
            s.execute("ALTER TABLE " + qualified + " REPLICA IDENTITY DEFAULT");
        } catch (Exception e) {
            Log.warn("could not reset REPLICA IDENTITY on %s: %s", qualified, e.getMessage());
        }
    }

    private static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (arg.equals(flag)) {
                return true;
            }
        }
        return false;
    }
}
