package io.tierdb.tiering;

import io.tierdb.catalog.RegisteredTable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/** The hot table's write frontier: {@code max(tier_key)}, or null when empty. */
public final class HotHighWater {

    public static Long query(DataSource dataSource, RegisteredTable meta) {
        String sql = "SELECT max(" + ident(meta.tierKeyCol()) + ") FROM "
                + ident(meta.schemaName()) + "." + ident(meta.tableName());
        try (Connection c = dataSource.getConnection();
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            Object v = rs.getObject(1);
            return v == null ? null : meta.tierKeyType().encode(v);
        } catch (SQLException e) {
            throw new TieringException("high-water probe failed for " + meta.tableName(), e);
        }
    }

    static String ident(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private HotHighWater() {}
}
