package io.tierdb.worker.cli;

import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.TableMode;
import io.tierdb.cdc.CdcException;
import io.tierdb.cdc.ReplicationSource;
import io.tierdb.lake.LakeStorage;
import io.tierdb.worker.LakeStorages;
import io.tierdb.worker.Log;
import io.tierdb.worker.WorkerConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import javax.sql.DataSource;

/** The offboarding command ({@code tierdb-worker unregister}). */
public final class TableUnregistrar {

    private TableUnregistrar() {}

    public static void run(WorkerConfig config, String[] args) throws Exception {
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
                .orElse(TableRegistrar.replicationName("tierdb_pub", schema, table));
        String slot = registered.map(RegisteredTable::slotName)
                .orElse(TableRegistrar.replicationName("tierdb_slot", schema, table));

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
            if (registered.map(RegisteredTable::keepHeap).orElse(false)) {
                dropColdMirrorTriggers(admin, registered.get().id().oid());
            }
        }

        if (registered.isEmpty()) {
            Log.info("%s is not registered; cleaned up any leftover slot/publication", qualified);
            return;
        }

        RegisteredTable meta = registered.get();

        if (dropLake) {
            LakeStorage lake = new LakeStorages(config, catalog).forTable(meta);
            lake.dropTable(meta.lakeTableRef());
            Log.info("dropped lake table %s (data files purged)", meta.lakeTableRef());
        } else if (meta.mode() == TableMode.TIERED && !meta.keepHeap()) {
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

    private static void dropColdMirrorTriggers(Connection admin, long tableOid) {
        try (Statement s = admin.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT to_regprocedure('tierdb_detach_cold_mirror(oid)') IS NOT NULL")) {
                if (!rs.next() || !rs.getBoolean(1)) {
                    return;
                }
            }
            try (ResultSet rs = s.executeQuery(
                    "SELECT tierdb_detach_cold_mirror(" + tableOid + "::oid)")) {
                if (rs.next()) {
                    Log.info("dropped %d cold-mirror trigger(s)", rs.getLong(1));
                }
            }
        } catch (Exception e) {
            Log.warn("could not drop cold-mirror triggers: %s", e.getMessage());
        }
    }

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
