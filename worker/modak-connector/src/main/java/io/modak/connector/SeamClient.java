package io.modak.connector;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * The JDBC half of the seam protocol: captures the seam state, atomically
 * with a {@code modak.read_pins} row when pinned. Everything runs in one
 * REPEATABLE READ transaction so the pin and the state describe the same seam.
 */
public final class SeamClient {

    private static final String TABLE_SQL = """
            SELECT t.table_id, t.primary_key_cols, t.tier_key_col, t.mode,
                   t.lake_format, t.lake_table_ref,
                   t.lake_props ->> 'metadata_location',
                   (t.lake_props ->> 'snapshot_id')::bigint,
                   t.heap_retention_lag
              FROM modak.tables t
             WHERE t.schema_name = ? AND t.table_name = ?
            """;

    private static final String CUTLINE_SQL =
            "SELECT tier_key_hi, retention_line, lake_snapshot_id"
                    + " FROM modak.cutline WHERE table_id = ?";

    private static final String PIN_SQL = """
            INSERT INTO modak.read_pins
                (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at)
            VALUES (?, ?, ?, now() + make_interval(secs => ?))
            RETURNING pin_id
            """;

    private static final String HYBRID_ELIGIBLE_SQL = """
            SELECT t.mode = 'mirrored' AND t.heap_retention_lag IS NULL
              FROM modak.tables t
             WHERE t.schema_name = ? AND t.table_name = ?
            """;

    private static final String FRONTIER_SQL = """
            SELECT c.replicated_lsn IS NOT NULL AND c.replicated_lsn >= ?
              FROM modak.cutline c JOIN modak.tables t USING (table_id)
             WHERE t.schema_name = ? AND t.table_name = ?
            """;

    private SeamClient() {}

    public static SeamState capture(SeamOptions options, boolean pin) {
        try (Connection c = connect(options)) {
            boolean hybrid = options.hybrid() && hybridEligible(c, options)
                    && waitForFrontier(c, options);
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            try {
                SeamState state = captureIn(c, options, pin, hybrid);
                c.commit();
                return state;
            } catch (Exception e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("seam capture failed for " + options.qualifiedName(), e);
        }
    }

    public static void release(SeamOptions options, long pinId) {
        try (Connection c = connect(options);
                PreparedStatement ps = c.prepareStatement(
                        "DELETE FROM modak.read_pins WHERE pin_id = ?")) {
            ps.setLong(1, pinId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("pin release failed (pin_id=" + pinId
                    + "); expires_at will reclaim it", e);
        }
    }

    /**
     * True when the table is mirrored without heap retention, the only shape a
     * hybrid read applies to.
     */
    private static boolean hybridEligible(Connection c, SeamOptions options) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(HYBRID_ELIGIBLE_SQL)) {
            ps.setString(1, options.schemaName());
            ps.setString(2, options.tableName());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }

    /**
     * Waits (bounded by {@link SeamOptions#mirrorWait}) for the mirror frontier
     * to pass the WAL position observed at call time, so the lake provably
     * holds everything committed before this capture. False means fall back to
     * the heap.
     */
    private static boolean waitForFrontier(Connection c, SeamOptions options) throws SQLException {
        long target;
        try (PreparedStatement ps = c.prepareStatement(
                        "SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint");
                ResultSet rs = ps.executeQuery()) {
            rs.next();
            target = rs.getLong(1);
        }
        long deadline = System.nanoTime() + options.mirrorWait().toNanos();
        while (true) {
            try (PreparedStatement ps = c.prepareStatement(FRONTIER_SQL)) {
                ps.setLong(1, target);
                ps.setString(2, options.schemaName());
                ps.setString(3, options.tableName());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && rs.getBoolean(1)) {
                        return true;
                    }
                }
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    private static SeamState captureIn(Connection c, SeamOptions options, boolean pin,
            boolean hybrid) throws SQLException {
        long tableId;
        List<String> pkCols;
        String tierKeyCol;
        String mode;
        String lakeFormat;
        String lakeTableRef;
        String metadataLocation;
        Long snapshotId;
        Long heapRetentionLag;

        try (PreparedStatement ps = c.prepareStatement(TABLE_SQL)) {
            ps.setString(1, options.schemaName());
            ps.setString(2, options.tableName());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            options.qualifiedName() + " is not registered in modak.tables");
                }
                tableId = rs.getLong(1);
                pkCols = textArray(rs.getArray(2));
                tierKeyCol = rs.getString(3);
                mode = rs.getString(4);
                lakeFormat = rs.getString(5);
                lakeTableRef = rs.getString(6);
                metadataLocation = rs.getString(7);
                snapshotId = (Long) rs.getObject(8);
                heapRetentionLag = (Long) rs.getObject(9);
            }
        }

        long tierKeyHi;
        Long retentionLine;
        long cutlineSnapshot;
        try (PreparedStatement ps = c.prepareStatement(CUTLINE_SQL)) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            options.qualifiedName() + " has no modak.cutline row");
                }
                tierKeyHi = rs.getLong(1);
                retentionLine = (Long) rs.getObject(2);
                cutlineSnapshot = rs.getLong(3);
            }
        }

        Long hybridSeam = null;
        if (hybrid && snapshotId != null) {
            hybridSeam = hybridSeam(c, options, tierKeyCol);
        }

        Long pinId = null;
        if (pin) {
            long pinnedSeam = hybridSeam != null ? hybridSeam : tierKeyHi;
            try (PreparedStatement ps = c.prepareStatement(PIN_SQL)) {
                ps.setLong(1, tableId);
                ps.setLong(2, cutlineSnapshot);
                ps.setLong(3, pinnedSeam);
                ps.setLong(4, options.pinTtl().toSeconds());
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    pinId = rs.getLong(1);
                }
            }
        }

        return new SeamState(tableId, pkCols, tierKeyCol, mode, lakeFormat, lakeTableRef,
                metadataLocation, snapshotId, heapRetentionLag, tierKeyHi, retentionLine,
                hybridSeam, pinId);
    }

    /**
     * The seam a hybrid read splits at: {@code max(tier_key) - hybridLag} from
     * the heap. Null (heap fallback) when the table is empty.
     */
    private static Long hybridSeam(Connection c, SeamOptions options, String tierKeyCol)
            throws SQLException {
        String sql = "SELECT max(" + quoteIdent(tierKeyCol) + ") FROM "
                + quoteIdent(options.schemaName()) + "." + quoteIdent(options.tableName());
        try (PreparedStatement ps = c.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) {
                return null;
            }
            Long highWater = (Long) rs.getObject(1);
            return highWater == null ? null : highWater - options.hybridLag();
        }
    }

    private static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    public static Connection connect(SeamOptions options) throws SQLException {
        return DriverManager.getConnection(options.jdbcUrl(), options.jdbcProperties());
    }

    private static List<String> textArray(Array array) throws SQLException {
        List<String> out = new ArrayList<>();
        for (Object o : (Object[]) array.getArray()) {
            out.add(String.valueOf(o));
        }
        return out;
    }
}
