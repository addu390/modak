package io.modak.load;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

/**
 * Hot rows into the heap: COPY into a temp table, then UPDATE-by-join plus an
 * anti-join INSERT. No {@code ON CONFLICT}, the primary key is logical and
 * partitioned tables carry no matching unique constraint.
 */
final class HeapLoader {

    private static final String TEMP_TABLE = "modak_load_hot";

    private HeapLoader() {}

    static long upsert(Connection c, String schemaName, String tableName, List<String> pkCols,
            List<String> columns, List<Map<String, Object>> rows) throws SQLException {
        if (rows.isEmpty()) {
            return 0;
        }
        String target = quote(schemaName) + "." + quote(tableName);
        String columnList = columns.stream().map(HeapLoader::quote)
                .collect(Collectors.joining(", "));

        try (Statement s = c.createStatement()) {
            s.execute("CREATE TEMP TABLE " + TEMP_TABLE + " (LIKE " + target + ") ON COMMIT DROP");
        }
        CopyManager copy = c.unwrap(PGConnection.class).getCopyAPI();
        try {
            copy.copyIn("COPY " + TEMP_TABLE + " (" + columnList + ") FROM STDIN",
                    new StringReader(copyText(columns, rows)));
        } catch (Exception e) {
            throw e instanceof SQLException se ? se
                    : new SQLException("COPY into " + TEMP_TABLE + " failed", e);
        }

        String pkJoin = pkCols.stream()
                .map(col -> "t." + quote(col) + " = x." + quote(col))
                .collect(Collectors.joining(" AND "));
        String updates = columns.stream()
                .filter(col -> !pkCols.contains(col))
                .map(col -> quote(col) + " = x." + quote(col))
                .collect(Collectors.joining(", "));
        long applied = 0;
        try (Statement s = c.createStatement()) {
            if (!updates.isEmpty()) {
                applied += s.executeUpdate("UPDATE " + target + " t SET " + updates
                        + " FROM " + TEMP_TABLE + " x WHERE " + pkJoin);
            }
            applied += s.executeUpdate("INSERT INTO " + target + " (" + columnList + ") "
                    + "SELECT " + columnList + " FROM " + TEMP_TABLE + " x "
                    + "WHERE NOT EXISTS (SELECT 1 FROM " + target + " t WHERE " + pkJoin + ")");
        }
        return applied;
    }

    private static String copyText(List<String> columns, List<Map<String, Object>> rows) {
        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> row : rows) {
            for (int i = 0; i < columns.size(); i++) {
                if (i > 0) {
                    sb.append('\t');
                }
                appendValue(sb, row.get(columns.get(i)));
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    private static void appendValue(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("\\N");
            return;
        }
        if (v instanceof Boolean b) {
            sb.append(b ? 't' : 'f');
            return;
        }
        String text = String.valueOf(v);
        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);
            switch (ch) {
                case '\\' -> sb.append("\\\\");
                case '\t' -> sb.append("\\t");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                default -> sb.append(ch);
            }
        }
    }

    private static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
