package io.modak.tiering;

import io.modak.catalog.Catalog;
import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TierKey;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Discovers child partitions of a range-partitioned table from {@code pg_inherits}
 * and registers unknown ones as HOT. Insert-only, it never touches the state
 * of a partition the workers already manage.
 */
public final class PartitionSync {

    // pg_get_expr(relpartbound) yields FOR VALUES FROM ('0') TO ('100'), quotes optional.
    static final Pattern RANGE_BOUND =
            Pattern.compile("FOR VALUES FROM \\('?(-?\\d+)'?\\) TO \\('?(-?\\d+)'?\\)");

    private static final String CHILDREN_SQL = """
            SELECT c.relname, pg_get_expr(c.relpartbound, c.oid) AS bound
              FROM pg_inherits i
              JOIN pg_class c ON c.oid = i.inhrelid
             WHERE i.inhparent = ?::regclass
             ORDER BY c.relname
            """;

    private final DataSource dataSource;
    private final Catalog catalog;

    public PartitionSync(DataSource dataSource, Catalog catalog) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.catalog = Objects.requireNonNull(catalog);
    }

    /**
     * Width ({@code hi - lo}) of the table's first PG range partition, the
     * natural lake partition granularity for a tiered table. Empty when the
     * table has no range partitions.
     */
    public static java.util.OptionalLong firstRangeWidth(DataSource ds, String qualified) {
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(CHILDREN_SQL)) {
            ps.setString(1, qualified);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Matcher m = RANGE_BOUND.matcher(rs.getString(2));
                    if (m.find()) {
                        return java.util.OptionalLong.of(
                                Long.parseLong(m.group(2)) - Long.parseLong(m.group(1)));
                    }
                }
            }
        } catch (SQLException e) {
            throw new TieringException("partition width inference failed for " + qualified, e);
        }
        return java.util.OptionalLong.empty();
    }

    /** Returns the number of newly registered partitions. */
    public int sync(RegisteredTable table) {
        Set<String> known = new HashSet<>();
        for (PartitionInfo p : catalog.listPartitions(table.id())) {
            known.add(p.id().id());
        }

        String qualified = table.schemaName() + "." + table.tableName();
        int added = 0;
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(CHILDREN_SQL)) {
            ps.setString(1, qualified);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (known.contains(name)) {
                        continue;
                    }
                    Matcher m = RANGE_BOUND.matcher(rs.getString(2));
                    if (!m.find()) {
                        continue; // DEFAULT / MINVALUE / MAXVALUE partitions are not tierable
                    }
                    catalog.upsertPartition(
                            new PartitionId(table.id(), name),
                            new PartitionBounds(
                                    new TierKey(Long.parseLong(m.group(1))),
                                    new TierKey(Long.parseLong(m.group(2)))),
                            PartitionState.HOT);
                    added++;
                }
            }
        } catch (SQLException e) {
            throw new TieringException("partition sync failed for " + qualified, e);
        }
        return added;
    }
}
