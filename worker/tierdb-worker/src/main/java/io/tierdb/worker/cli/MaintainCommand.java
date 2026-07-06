package io.tierdb.worker.cli;

import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.worker.WorkerConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import javax.sql.DataSource;

/** The {@code tierdb-worker maintain} command: files a maintenance request and waits for the leader to journal the run. */
public final class MaintainCommand {

    private static final long WAIT_MILLIS = 120_000;

    private MaintainCommand() {}

    public static int run(WorkerConfig config, String[] args) throws Exception {
        Args parsed = new Args(args);
        String qualified = parsed.required("--table");
        String[] parts = qualified.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("--table must be schema.table: " + qualified);
        }

        DataSource dataSource = config.dataSource();
        JdbcCatalog catalog = new JdbcCatalog(dataSource);
        RegisteredTable table = catalog.lookup(parts[0], parts[1])
                .orElseThrow(() -> new IllegalArgumentException(
                        "table is not registered: " + qualified));

        Timestamp requestedAt = dbNow(dataSource);
        catalog.requestMaintenance(table.id(), "cli");
        System.out.println("maintenance requested for " + qualified
                + ", the leader picks it up within its cycle interval");
        if (parsed.has("--no-wait")) {
            return 0;
        }

        long deadline = System.currentTimeMillis() + WAIT_MILLIS;
        while (System.currentTimeMillis() < deadline) {
            String details = journaledPass(dataSource, table.id().oid(), requestedAt);
            if (details != null) {
                System.out.println("maintenance ran: " + details);
                return 0;
            }
            Thread.sleep(1000);
        }
        System.err.println("timed out waiting for the pass, is a worker running and leading? "
                + "The request stays pending and runs when a leader appears.");
        return 1;
    }

    private static Timestamp dbNow(DataSource dataSource) throws Exception {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("SELECT now()");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getTimestamp(1);
        }
    }

    private static String journaledPass(DataSource dataSource, long tableId,
            Timestamp requestedAt) throws Exception {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement("""
                        SELECT details::text FROM tierdb.op_log
                         WHERE table_id = ? AND op_kind = 'maintenance' AND updated_at >= ?
                         ORDER BY updated_at DESC LIMIT 1
                        """)) {
            ps.setLong(1, tableId);
            ps.setTimestamp(2, requestedAt);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
