package io.modak.compaction;

import io.modak.catalog.Catalog;
import io.modak.catalog.RegisteredTable;
import io.modak.common.DeltaBatch;
import io.modak.common.DeltaRowsBatch;
import io.modak.common.PgValues;
import io.modak.common.RowBatchData.Column;
import io.modak.common.TableId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * {@link CompactionPolicy} over JDBC. Folds eagerly, selecting up to
 * {@code maxBatchSize} delta rows, oldest version first. Row images are
 * extracted from the jsonb payload by Postgres itself, no JSON handling in Java.
 */
public final class JdbcCompactionPolicy implements CompactionPolicy {

    private final DataSource dataSource;
    private final Catalog catalog;
    private final int maxBatchSize;

    public JdbcCompactionPolicy(DataSource dataSource, Catalog catalog, int maxBatchSize) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.catalog = Objects.requireNonNull(catalog);
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public Optional<DeltaBatch> selectForCompaction(TableId table, Instant now) {
        RegisteredTable meta = catalog.get(table)
                .orElseThrow(() -> new IllegalStateException("table not registered: " + table));
        try (Connection c = dataSource.getConnection()) {
            List<Column> columns = columnsOf(c, meta);
            List<DeltaRowsBatch.Entry> entries = selectEntries(c, meta, columns);
            if (entries.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new DeltaRowsBatch(table, meta.primaryKeyCols(), columns, entries));
        } catch (SQLException e) {
            throw new IllegalStateException("failed to select delta batch for " + table, e);
        }
    }

    private static final String COLUMNS_SQL = """
            SELECT column_name, data_type,
                   COALESCE(numeric_precision, 0), COALESCE(numeric_scale, 0)
              FROM information_schema.columns
             WHERE table_schema = ? AND table_name = ?
             ORDER BY ordinal_position
            """;

    private static List<Column> columnsOf(Connection c, RegisteredTable meta) throws SQLException {
        List<Column> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(COLUMNS_SQL)) {
            ps.setString(1, meta.schemaName());
            ps.setString(2, meta.tableName());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(PgValues.column(rs.getString(1), rs.getString(2),
                            rs.getInt(3), rs.getInt(4)));
                }
            }
        }
        if (out.isEmpty()) {
            throw new SQLException("hot relation not found: "
                    + meta.schemaName() + "." + meta.tableName());
        }
        return out;
    }

    private List<DeltaRowsBatch.Entry> selectEntries(Connection c, RegisteredTable meta,
            List<Column> columns) throws SQLException {
        StringBuilder sql = new StringBuilder(
                "SELECT d.pk, d.op, d.tier_key, d.version, (d.payload IS NULL), d.old_tier_key");
        for (Column col : columns) {
            sql.append(", (d.payload ->> ").append(lit(col.name())).append(')')
                    .append(PgValues.castSuffix(col.type()));
        }
        sql.append(" FROM modak.delta d WHERE d.table_id = ? ORDER BY d.version LIMIT ?");

        List<DeltaRowsBatch.Entry> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
            ps.setLong(1, meta.id().oid());
            ps.setInt(2, maxBatchSize);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    boolean tombstone = rs.getShort(2) == 1;
                    boolean noPayload = rs.getBoolean(5);
                    Long oldTierKey = (Long) rs.getObject(6);
                    // Tombstone payloads carry the pk fields, legacy tombstones have none.
                    Object[] row = null;
                    if (!noPayload) {
                        row = new Object[columns.size()];
                        for (int i = 0; i < columns.size(); i++) {
                            row[i] = PgValues.readValue(rs, 7 + i, columns.get(i).type());
                        }
                    }
                    out.add(new DeltaRowsBatch.Entry(rs.getString(1), tombstone,
                            rs.getLong(3), oldTierKey, rs.getLong(4), row));
                }
            }
        }
        return out;
    }

    private static String lit(String s) {
        return "'" + s.replace("'", "''") + "'";
    }
}
