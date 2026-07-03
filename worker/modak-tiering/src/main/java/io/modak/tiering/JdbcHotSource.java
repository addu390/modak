package io.modak.tiering;

import io.modak.catalog.PartitionInfo;
import io.modak.catalog.RegisteredTable;
import io.modak.common.PartitionData;
import io.modak.common.PartitionId;
import io.modak.common.PgValues;
import io.modak.common.RowBatchData;
import io.modak.common.RowBatchData.Column;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;

/**
 * {@link HotSource} over JDBC. {@code PartitionId.id()} is the physical child
 * relation of a range-partitioned table, reads select from it, reclaim DROPs it.
 * Reads re-apply the tier-key bounds so a mis-registered partition row can never
 * leak rows across the cut-line.
 */
public final class JdbcHotSource implements HotSource {

    private final DataSource dataSource;

    public JdbcHotSource(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    @Override
    public PartitionData read(RegisteredTable table, PartitionInfo partition) {
        String sql = "SELECT * FROM " + ident(table.schemaName()) + "." + ident(partition.id().id())
                + " WHERE " + ident(table.tierKeyCol()) + " >= ? AND " + ident(table.tierKeyCol()) + " < ?";
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, partition.bounds().lo().value());
            ps.setLong(2, partition.bounds().hi().value());
            try (ResultSet rs = ps.executeQuery()) {
                List<Column> columns = columnsOf(rs.getMetaData());
                List<Object[]> rows = new ArrayList<>();
                while (rs.next()) {
                    Object[] row = new Object[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        row[i] = PgValues.readValue(rs, i + 1, columns.get(i).type());
                    }
                    rows.add(row);
                }
                return new RowBatchData(partition.id(), partition.bounds(), columns, rows);
            }
        } catch (SQLException e) {
            throw new TieringException("failed to read hot partition " + partition.id(), e);
        }
    }

    @Override
    public void dropPartition(RegisteredTable table, PartitionId partition) {
        // IF EXISTS keeps reclaim idempotent across crashes.
        String sql = "DROP TABLE IF EXISTS " + ident(table.schemaName()) + "." + ident(partition.id());
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            throw new TieringException("failed to drop partition " + partition, e);
        }
    }

    private static List<Column> columnsOf(ResultSetMetaData meta) throws SQLException {
        List<Column> out = new ArrayList<>(meta.getColumnCount());
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            out.add(PgValues.column(meta.getColumnName(i), meta.getColumnTypeName(i),
                    meta.getPrecision(i), meta.getScale(i)));
        }
        return out;
    }

    private static String ident(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }
}
