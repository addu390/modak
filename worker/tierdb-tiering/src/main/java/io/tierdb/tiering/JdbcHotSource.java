package io.tierdb.tiering;

import io.tierdb.catalog.PartitionInfo;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.PartitionData;
import io.tierdb.common.PartitionId;
import io.tierdb.common.PgValues;
import io.tierdb.common.RowBatchData;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.TierKeyType;
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
            TierKeyType codec = table.tierKeyType();
            ps.setObject(1, codec.decode(partition.bounds().lo().value()));
            ps.setObject(2, codec.decode(partition.bounds().hi().value()));
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
    public void attachColdMirror(RegisteredTable table, PartitionId partition) {
        String qualified = ident(table.schemaName()) + "." + ident(partition.id());
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery(
                    "SELECT to_regprocedure('tierdb_attach_cold_mirror(oid, oid)') IS NOT NULL")) {
                if (!rs.next() || !rs.getBoolean(1)) {
                    throw new TieringException("keep-heap table " + table.schemaName() + "."
                            + table.tableName() + " needs the tierdb extension: without the "
                            + "cold-mirror trigger, plain DML below the cut-line would "
                            + "silently diverge from the lake");
                }
            }
            try (ResultSet rs = s.executeQuery("SELECT tierdb_attach_cold_mirror("
                    + table.id().oid() + "::oid, '" + qualified.replace("'", "''")
                    + "'::regclass::oid)")) {
                rs.next();
            }
        } catch (SQLException e) {
            throw new TieringException("failed to attach the cold mirror on " + partition, e);
        }
    }

    @Override
    public void dropPartition(RegisteredTable table, PartitionId partition) {
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
