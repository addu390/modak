package io.modak.tiering;

import io.modak.catalog.RegisteredTable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import javax.sql.DataSource;

/**
 * Creates future heap partitions ahead of the write frontier so inserts never
 * land without one. Unit-agnostic: the width comes from the topmost existing
 * range partition, and at least {@code headroom} empty widths are kept between
 * the hot high-water and the top bound.
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

    /** Empty when the table has no range partitions (nothing to extend). */
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
        long[] top = null;
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(CHILDREN_SQL)) {
            ps.setString(1, qualified);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Matcher m = PartitionSync.RANGE_BOUND.matcher(rs.getString(2));
                    if (!m.find()) {
                        continue;
                    }
                    long lo = Long.parseLong(m.group(1));
                    long hi = Long.parseLong(m.group(2));
                    if (top == null || hi > top[1]) {
                        top = new long[] {lo, hi};
                    }
                }
            }
        } catch (SQLException e) {
            throw new TieringException("partition discovery failed for " + qualified, e);
        }
        return top;
    }

    private void createPartition(RegisteredTable table, long lo, long hi) {
        String suffix = lo < 0 ? "pm" + (-lo) : "p" + lo;
        String name = table.tableName() + "_" + suffix;
        String sql = "CREATE TABLE IF NOT EXISTS "
                + HotHighWater.ident(table.schemaName()) + "." + HotHighWater.ident(name)
                + " PARTITION OF "
                + HotHighWater.ident(table.schemaName()) + "." + HotHighWater.ident(table.tableName())
                + " FOR VALUES FROM (" + lo + ") TO (" + hi + ")";
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            throw new TieringException("premake failed for " + name, e);
        }
    }
}
