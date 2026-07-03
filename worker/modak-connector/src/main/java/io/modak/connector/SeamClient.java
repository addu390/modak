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
            "SELECT tier_key_hi, retention_line FROM modak.cutline WHERE table_id = ?";

    private static final String PIN_SQL = """
            INSERT INTO modak.read_pins
                (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at)
            SELECT c.table_id, c.lake_snapshot_id, c.tier_key_hi,
                   now() + make_interval(secs => ?)
              FROM modak.cutline c
             WHERE c.table_id = ?
            RETURNING pin_id, pinned_tier_key_hi
            """;

    private SeamClient() {}

    public static SeamState capture(SeamOptions options, boolean pin) {
        try (Connection c = connect(options)) {
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            try {
                SeamState state = captureIn(c, options, pin);
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

    private static SeamState captureIn(Connection c, SeamOptions options, boolean pin)
            throws SQLException {
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

        Long pinId = null;
        if (pin) {
            try (PreparedStatement ps = c.prepareStatement(PIN_SQL)) {
                ps.setLong(1, options.pinTtl().toSeconds());
                ps.setLong(2, tableId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new IllegalStateException(
                                options.qualifiedName() + " has no modak.cutline row");
                    }
                    pinId = rs.getLong(1);
                }
            }
        }

        long tierKeyHi;
        Long retentionLine;
        try (PreparedStatement ps = c.prepareStatement(CUTLINE_SQL)) {
            ps.setLong(1, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException(
                            options.qualifiedName() + " has no modak.cutline row");
                }
                tierKeyHi = rs.getLong(1);
                retentionLine = (Long) rs.getObject(2);
            }
        }

        return new SeamState(tableId, pkCols, tierKeyCol, mode, lakeFormat, lakeTableRef,
                metadataLocation, snapshotId, heapRetentionLag, tierKeyHi, retentionLine, pinId);
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
