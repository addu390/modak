package io.tierdb.tiering;

import io.tierdb.catalog.Catalog;
import io.tierdb.catalog.PartitionInfo;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.PartitionBounds;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;
import io.tierdb.common.TierKey;
import io.tierdb.common.TierKeyType;
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

    static final Pattern RANGE_BOUND =
            Pattern.compile("FOR VALUES FROM \\('?([^')]+)'?\\) TO \\('?([^')]+)'?\\)");

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

    public static java.util.OptionalLong firstRangeWidth(DataSource ds, String qualified,
            TierKeyType codec) {
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement(CHILDREN_SQL)) {
            ps.setString(1, qualified);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.util.OptionalLong width = boundsOf(rs.getString(2), codec)
                            .map(b -> java.util.OptionalLong.of(b[1] - b[0]))
                            .orElse(java.util.OptionalLong.empty());
                    if (width.isPresent()) {
                        return width;
                    }
                }
            }
        } catch (SQLException e) {
            throw new TieringException("partition width inference failed for " + qualified, e);
        }
        return java.util.OptionalLong.empty();
    }

    static java.util.Optional<long[]> boundsOf(String bound, TierKeyType codec) {
        Matcher m = RANGE_BOUND.matcher(bound);
        if (!m.find()) {
            return java.util.Optional.empty();
        }
        try {
            return java.util.Optional.of(new long[] {
                    codec.parseBoundLiteral(m.group(1)),
                    codec.parseBoundLiteral(m.group(2))});
        } catch (RuntimeException e) {
            return java.util.Optional.empty();
        }
    }

    public int sync(RegisteredTable table) {
        Set<String> known = new HashSet<>();
        for (PartitionInfo p : catalog.listPartitions(table.id())) {
            known.add(p.id().id());
        }

        String qualified = table.schemaName() + "." + table.tableName();
        TierKeyType codec = table.tierKeyType();
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
                    java.util.Optional<long[]> bounds = boundsOf(rs.getString(2), codec);
                    if (bounds.isEmpty()) {
                        continue;
                    }
                    catalog.upsertPartition(
                            new PartitionId(table.id(), name),
                            new PartitionBounds(
                                    new TierKey(bounds.get()[0]),
                                    new TierKey(bounds.get()[1])),
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
