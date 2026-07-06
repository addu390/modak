package io.tierdb.catalog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * Minimal JDBC helper that collapses the connect / prepare / try-with-resources
 * dance to a single call. No ORM, no dependencies, SQL stays as inline text
 * blocks in the caller.
 */
final class Db {

    /** Binds parameters onto a prepared statement. */
    @FunctionalInterface
    interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    /** Maps the current row of a result set to a value. */
    @FunctionalInterface
    interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    /** A unit of work running inside a single transaction. */
    @FunctionalInterface
    interface TxWork<T> {
        T run(Connection c) throws SQLException;
    }

    static final Binder NO_ARGS = ps -> {};

    private final DataSource dataSource;

    Db(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    int update(String sql, Binder binder) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw fail(sql, e);
        }
    }

    <T> Optional<T> queryOne(String sql, Binder binder, RowMapper<T> mapper) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.ofNullable(mapper.map(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw fail(sql, e);
        }
    }

    <T> List<T> queryList(String sql, Binder binder, RowMapper<T> mapper) {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(mapper.map(rs));
                }
                return out;
            }
        } catch (SQLException e) {
            throw fail(sql, e);
        }
    }

    <T> T inTransaction(TxWork<T> work) {
        try (Connection c = dataSource.getConnection()) {
            boolean prevAutoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                T result = work.run(c);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw (e instanceof SQLException se) ? fail("transaction", se) : (RuntimeException) e;
            } finally {
                c.setAutoCommit(prevAutoCommit);
            }
        } catch (SQLException e) {
            throw fail("transaction", e);
        }
    }

    private static CatalogException fail(String sql, SQLException e) {
        String firstLine = sql.strip().lines().findFirst().orElse(sql).strip();
        return new CatalogException("catalog SQL failed: " + firstLine, e);
    }
}
