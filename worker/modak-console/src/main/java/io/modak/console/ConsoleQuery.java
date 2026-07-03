package io.modak.console;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.StringJoiner;
import javax.sql.DataSource;

/**
 * The playground executor: runs operator-typed SQL on a bounded connection
 * (query timeout + row cap) and renders results as JSON. Runs with transparent
 * reads on, so tiered tables read as users see them.
 */
final class ConsoleQuery {

    static final int MAX_ROWS = 1000;
    static final int TIMEOUT_SECONDS = 30;

    private final DataSource dataSource;

    ConsoleQuery(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    String run(String sql) {
        long started = System.nanoTime();
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement()) {
            s.setQueryTimeout(TIMEOUT_SECONDS);
            boolean hasResultSet = s.execute(sql);
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            if (!hasResultSet) {
                return "{\"updateCount\":" + s.getUpdateCount()
                        + ",\"elapsedMs\":" + elapsedMs + "}";
            }
            try (ResultSet rs = s.getResultSet()) {
                return render(rs, started);
            }
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            return "{\"error\":" + Json.str(message(e))
                    + ",\"elapsedMs\":" + elapsedMs + "}";
        }
    }

    private static String render(ResultSet rs, long started) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        int n = meta.getColumnCount();

        StringJoiner columns = new StringJoiner(",", "[", "]");
        for (int i = 1; i <= n; i++) {
            columns.add("{\"name\":" + Json.str(meta.getColumnLabel(i))
                    + ",\"type\":" + Json.str(meta.getColumnTypeName(i)) + "}");
        }

        StringJoiner rows = new StringJoiner(",", "[", "]");
        int count = 0;
        boolean truncated = false;
        while (rs.next()) {
            if (count == MAX_ROWS) {
                truncated = true;
                break;
            }
            StringJoiner row = new StringJoiner(",", "[", "]");
            for (int i = 1; i <= n; i++) {
                String v = rs.getString(i);
                row.add(v == null ? "null" : Json.str(v));
            }
            rows.add(row.toString());
            count++;
        }
        long elapsedMs = (System.nanoTime() - started) / 1_000_000;
        return "{\"columns\":" + columns + ",\"rows\":" + rows
                + ",\"rowCount\":" + count
                + ",\"truncated\":" + Json.bool(truncated)
                + ",\"elapsedMs\":" + elapsedMs + "}";
    }

    /** Runs modak_explain over the statement and returns its report lines. */
    String explain(String sql) {
        long started = System.nanoTime();
        try (Connection c = dataSource.getConnection();
                var s = c.prepareStatement("SELECT modak_explain FROM modak_explain(?)")) {
            s.setQueryTimeout(TIMEOUT_SECONDS);
            s.setString(1, sql);
            StringJoiner lines = new StringJoiner(",", "[", "]");
            try (ResultSet rs = s.executeQuery()) {
                while (rs.next()) {
                    lines.add(Json.str(rs.getString(1)));
                }
            }
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            return "{\"lines\":" + lines + ",\"elapsedMs\":" + elapsedMs + "}";
        } catch (Exception e) {
            long elapsedMs = (System.nanoTime() - started) / 1_000_000;
            return "{\"error\":" + Json.str(message(e))
                    + ",\"elapsedMs\":" + elapsedMs + "}";
        }
    }

    private static final String SCHEMA = """
            SELECT table_schema, table_name, column_name, data_type
              FROM information_schema.columns
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
             ORDER BY table_schema, table_name, ordinal_position
            """;

    String schema() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(SCHEMA)) {
            StringJoiner tables = new StringJoiner(",", "[", "]");
            String currentTable = null;
            StringBuilder columns = null;
            while (rs.next()) {
                String table = rs.getString(1) + "." + rs.getString(2);
                if (!table.equals(currentTable)) {
                    if (columns != null) {
                        tables.add("{\"name\":" + Json.str(currentTable)
                                + ",\"columns\":[" + columns + "]}");
                    }
                    currentTable = table;
                    columns = new StringBuilder();
                }
                if (columns.length() > 0) {
                    columns.append(',');
                }
                columns.append("{\"name\":").append(Json.str(rs.getString(3)))
                        .append(",\"type\":").append(Json.str(rs.getString(4))).append('}');
            }
            if (columns != null) {
                tables.add("{\"name\":" + Json.str(currentTable)
                        + ",\"columns\":[" + columns + "]}");
            }
            return "{\"tables\":" + tables + "}";
        }
    }

    private static String message(Exception e) {
        String m = e.getMessage();
        return m == null || m.isBlank() ? e.toString() : m.strip();
    }
}
