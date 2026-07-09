package io.tierdb.lake.commit;

import io.tierdb.common.TableId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.sql.DataSource;

/**
 * The per-table lake commit lock shared by every direct-mode writer (worker,
 * pg extension, connectors). Session-scoped: held on a dedicated connection
 * until {@link #close()}, which also closes the connection.
 */
public final class LakeCommitLock implements AutoCloseable {

    private static final String LOCK_SQL =
            "SELECT pg_advisory_lock(hashtextextended('tierdb_lake_' || ?::text, 0))";
    private static final String UNLOCK_SQL =
            "SELECT pg_advisory_unlock(hashtextextended('tierdb_lake_' || ?::text, 0))";

    private final Connection connection;
    private final long tableId;

    private LakeCommitLock(Connection connection, long tableId) {
        this.connection = connection;
        this.tableId = tableId;
    }

    public static LakeCommitLock acquire(DataSource ds, TableId table) throws Exception {
        return acquire(ds.getConnection(), table.oid());
    }

    /** Takes ownership of {@code connection}; it is closed with the lock. */
    public static LakeCommitLock acquire(Connection connection, long tableId) throws Exception {
        try {
            run(connection, LOCK_SQL, tableId);
        } catch (Exception e) {
            connection.close();
            throw e;
        }
        return new LakeCommitLock(connection, tableId);
    }

    @Override
    public void close() throws Exception {
        try {
            run(connection, UNLOCK_SQL, tableId);
        } finally {
            connection.close();
        }
    }

    private static void run(Connection c, String sql, long tableId) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, Long.toString(tableId));
            ps.execute();
        }
    }
}
