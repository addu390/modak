package io.tierdb.load;

import io.tierdb.catalog.LoadState;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;

/**
 * The {@code tierdb.load_labels} SQL run on the load's own transaction, so the
 * label row commits atomically with the batch's writes.
 */
final class LabelRegistry {

    record Stored(LoadState state, String stagedFilesJson, String resultJson) {}

    private LabelRegistry() {}

    static boolean tryLock(Connection c, long tableId, String label) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(?, ?))")) {
            ps.setString(1, label);
            ps.setLong(2, tableId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        }
    }

    static boolean insert(Connection c, long tableId, String label, LoadState state,
            String stagedFilesJson, String resultJson) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("""
                INSERT INTO tierdb.load_labels (table_id, label, state, staged_files, result)
                VALUES (?, ?, ?, ?::jsonb, ?::jsonb)
                ON CONFLICT (table_id, label) DO NOTHING
                """)) {
            ps.setLong(1, tableId);
            ps.setString(2, label);
            ps.setString(3, state.sql());
            if (stagedFilesJson == null) {
                ps.setNull(4, Types.VARCHAR);
            } else {
                ps.setString(4, stagedFilesJson);
            }
            if (resultJson == null) {
                ps.setNull(5, Types.VARCHAR);
            } else {
                ps.setString(5, resultJson);
            }
            return ps.executeUpdate() > 0;
        }
    }

    static Optional<Stored> lookup(Connection c, long tableId, String label) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT state, staged_files, result FROM tierdb.load_labels "
                        + "WHERE table_id = ? AND label = ?")) {
            ps.setLong(1, tableId);
            ps.setString(2, label);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next()
                        ? Optional.of(new Stored(LoadState.fromSql(rs.getString(1)),
                                rs.getString(2), rs.getString(3)))
                        : Optional.empty();
            }
        }
    }
}
