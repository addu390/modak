package io.tierdb.worker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * The active/passive HA lease: one Postgres advisory lock held on a dedicated
 * connection. Losing the session loses the lease.
 */
final class LeaderLease {

    private static final long WORKER_LOCK_KEY = 0x6d6f64616bL; // "tierdb", arbitrary but stable

    private final DataSource dataSource;
    private Connection lockConnection;

    LeaderLease(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    boolean tryAcquire() throws Exception {
        Connection c = dataSource.getConnection();
        try (Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(
                        "SELECT pg_try_advisory_lock(" + WORKER_LOCK_KEY + ")")) {
            rs.next();
            if (rs.getBoolean(1)) {
                lockConnection = c;
                return true;
            }
        }
        c.close();
        return false;
    }

    boolean stillHeld() {
        try {
            return lockConnection != null && lockConnection.isValid(2);
        } catch (Exception e) {
            return false;
        }
    }

    void release() {
        if (lockConnection != null) {
            try {
                lockConnection.close();
            } catch (Exception ignored) {
            }
            lockConnection = null;
        }
    }
}
