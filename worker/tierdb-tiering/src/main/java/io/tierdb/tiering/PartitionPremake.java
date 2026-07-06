package io.tierdb.tiering;

import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.TierKeyType;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * Creates future heap partitions ahead of the write frontier so inserts
 * never land without one.
 */
public final class PartitionPremake {

    /**
     * {@code outsideGrid}: rows sit at or past the top range bound (a DEFAULT
     * partition), which blocks extending the grid.
     */
    public record Result(int created, boolean outsideGrid) {}

    private static final String CHILDREN_SQL = """
            SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) AS bound
              FROM pg_inherits i
              JOIN pg_class c ON c.oid = i.inhrelid
             WHERE i.inhparent = ?::regclass
            """;

    private final DataSource dataSource;
    private final int headroom;

    public PartitionPremake(DataSource dataSource, int headroom) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.headroom = headroom;
    }

    public Optional<Result> premake(RegisteredTable table) {
        if (headroom <= 0) {
            return Optional.of(new Result(0, false));
        }
        long[] top = topPartition(table);
        if (top == null) {
            return Optional.empty();
        }
        long width = top[1] - top[0];
        if (width <= 0) {
            return Optional.empty();
        }
        Long highWater = HotHighWater.query(dataSource, table);
        if (highWater == null) {
            return Optional.of(new Result(0, false));
        }

        long topHi = top[1];
        if (highWater >= topHi) {
            return Optional.of(new Result(0, true));
        }
        int created = 0;
        long need = highWater + (long) headroom * width;
        while (topHi < need) {
            createPartition(table, topHi, topHi + width);
            topHi += width;
            created++;
        }
        return Optional.of(new Result(created, false));
    }

    private long[] topPartition(RegisteredTable table) {
        String qualified = table.schemaName() + "." + table.tableName();
        TierKeyType codec = table.tierKeyType();
        long[] top = null;
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(CHILDREN_SQL)) {
            ps.setString(1, qualified);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Optional<long[]> bounds = PartitionSync.boundsOf(rs.getString(2), codec);
                    if (bounds.isEmpty()) {
                        continue;
                    }
                    if (top == null || bounds.get()[1] > top[1]) {
                        top = bounds.get();
                    }
                }
            }
        } catch (SQLException e) {
            throw new TieringException("partition discovery failed for " + qualified, e);
        }
        return top;
    }

    private void createPartition(RegisteredTable table, long lo, long hi) {
        TierKeyType codec = table.tierKeyType();
        String name = table.tableName() + "_p" + codec.nameToken(lo);
        String sql = "CREATE TABLE IF NOT EXISTS "
                + HotHighWater.ident(table.schemaName()) + "." + HotHighWater.ident(name)
                + " PARTITION OF "
                + HotHighWater.ident(table.schemaName()) + "." + HotHighWater.ident(table.tableName())
                + " FOR VALUES FROM (" + codec.pgLiteral(lo) + ") TO (" + codec.pgLiteral(hi) + ")";
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            throw new TieringException("premake failed for " + name, e);
        }
    }
}
