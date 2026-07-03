package io.modak.worker;

import io.modak.common.DeltaRowsBatch;
import io.modak.common.Lsn;
import io.modak.common.PgValues;
import io.modak.common.PkCodec;
import io.modak.common.RowBatchData.Column;
import io.modak.common.RowBatchData.ColumnType;
import io.modak.common.TableId;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeStorage;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/**
 * The mirrored-registration copy. PK-ordered chunks folded through the merge
 * writer (idempotent upserts), progress journaled in {@code modak.copy_progress}
 * after each chunk. A crashed copy resumes from the journal, and rows changed
 * while copying are healed by the stream replaying from the slot's consistent point.
 */
final class InitialCopy {

    private InitialCopy() {}

    /** Copies from the journaled position to the end, null when nothing was committed. */
    static LakeCommitResult run(DataSource ds, LakeStorage lake, TableId table,
            String schema, String tableName, String lakeTableRef,
            List<String> pkCols, String tierKeyCol, List<Column> columns,
            Lsn consistentPoint, int chunkRows) throws Exception {
        List<Column> pkColumns = pkColumns(pkCols, columns);
        List<String> lastPk = readLastPk(ds, table);
        LakeCommitResult last = null;
        long chunkNo = 0;
        while (true) {
            Chunk chunk = readChunk(ds, schema, tableName, pkColumns, tierKeyCol,
                    lastPk, chunkRows);
            if (chunk.entries().isEmpty()) {
                return last;
            }
            last = lake.table(new CommitterInitContext(table, lakeTableRef),
                            new ColdTableSpec(pkCols, tierKeyCol))
                    .mergeWriter()
                    .applyDelta(
                            new DeltaRowsBatch(table, pkCols, chunk.columns(), chunk.entries()),
                            MirrorWorker.mirrorProps(table, consistentPoint));
            lastPk = chunk.lastPkText();
            journal(ds, table, lastPk);
            Log.info("%s.%s: initial copy chunk %d (%d row(s)) committed",
                    schema, tableName, ++chunkNo, chunk.entries().size());
            if (chunk.entries().size() < chunkRows) {
                return last;
            }
        }
    }

    static void begin(DataSource ds, TableId table, Lsn consistentPoint) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "INSERT INTO modak.copy_progress (table_id, consistent_point) VALUES (?, ?)")) {
            ps.setLong(1, table.oid());
            ps.setLong(2, consistentPoint.value());
            ps.executeUpdate();
        }
    }

    /** The consistent point of an in-flight copy, or null when none is journaled. */
    static Lsn inFlightConsistentPoint(DataSource ds, TableId table) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT consistent_point FROM modak.copy_progress WHERE table_id = ?")) {
            ps.setLong(1, table.oid());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new Lsn(rs.getLong(1)) : null;
            }
        }
    }

    static void finish(DataSource ds, TableId table) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "DELETE FROM modak.copy_progress WHERE table_id = ?")) {
            ps.setLong(1, table.oid());
            ps.executeUpdate();
        }
    }

    private record Chunk(List<Column> columns, List<DeltaRowsBatch.Entry> entries,
            List<String> lastPkText) {}

    private static Chunk readChunk(DataSource ds, String schema, String tableName,
            List<Column> pkColumns, String tierKeyCol, List<String> lastPk, int chunkRows)
            throws Exception {
        String sql = chunkSql(schema, tableName, pkColumns, lastPk != null, chunkRows);
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            if (lastPk != null) {
                for (int i = 0; i < lastPk.size(); i++) {
                    ps.setString(i + 1, lastPk.get(i));
                }
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<Column> columns = columnsOf(rs);
                int[] pkIdx = new int[pkColumns.size()];
                for (int i = 0; i < pkColumns.size(); i++) {
                    pkIdx[i] = indexOf(columns, pkColumns.get(i).name());
                }
                int tierIdx = indexOf(columns, tierKeyCol);
                List<DeltaRowsBatch.Entry> entries = new ArrayList<>();
                Object[] row = null;
                long version = 0;
                while (rs.next()) {
                    row = new Object[columns.size()];
                    for (int i = 0; i < columns.size(); i++) {
                        row[i] = PgValues.readValue(rs, i + 1, columns.get(i).type());
                    }
                    entries.add(new DeltaRowsBatch.Entry(
                            encodePk(row, pkIdx), false,
                            tierKeyOf(row[tierIdx]), ++version, row));
                }
                List<String> lastText = row == null ? null : pkText(row, pkIdx, pkColumns);
                return new Chunk(columns, entries, lastText);
            }
        }
    }

    // Keyset pagination: a crash re-read never skips, never repeats beyond one chunk.
    private static String chunkSql(String schema, String tableName, List<Column> pkColumns,
            boolean resuming, int chunkRows) {
        StringBuilder order = new StringBuilder();
        StringBuilder lhs = new StringBuilder();
        StringBuilder rhs = new StringBuilder();
        for (int i = 0; i < pkColumns.size(); i++) {
            if (i > 0) {
                order.append(", ");
                lhs.append(", ");
                rhs.append(", ");
            }
            order.append(ident(pkColumns.get(i).name()));
            lhs.append(ident(pkColumns.get(i).name()));
            String cast = PgValues.castSuffix(pkColumns.get(i).type());
            rhs.append('?').append(cast.isEmpty() ? "::text" : cast);
        }
        String where = resuming
                ? " WHERE ROW(" + lhs + ") > ROW(" + rhs + ")"
                : "";
        return "SELECT * FROM " + ident(schema) + "." + ident(tableName)
                + where + " ORDER BY " + order + " LIMIT " + chunkRows;
    }

    private static List<Column> pkColumns(List<String> pkCols, List<Column> columns) {
        List<Column> out = new ArrayList<>(pkCols.size());
        for (String pk : pkCols) {
            out.add(columns.stream().filter(c -> c.name().equals(pk)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException(
                            "pk column '" + pk + "' not found: " + columns)));
        }
        return out;
    }

    private static String encodePk(Object[] row, int[] pkIdx) {
        List<String> parts = new ArrayList<>(pkIdx.length);
        for (int idx : pkIdx) {
            Object v = row[idx];
            if (v == null) {
                throw new IllegalStateException("pk column is NULL in the initial copy");
            }
            parts.add(String.valueOf(v));
        }
        return PkCodec.encode(parts);
    }

    private static List<String> pkText(Object[] row, int[] pkIdx, List<Column> pkColumns) {
        List<String> out = new ArrayList<>(pkIdx.length);
        for (int i = 0; i < pkIdx.length; i++) {
            out.add(toPgText(row[pkIdx[i]], pkColumns.get(i).type()));
        }
        return out;
    }

    // Text a PG cast parses back to the same value, only bytea needs special form.
    private static String toPgText(Object value, ColumnType type) {
        return type == ColumnType.BINARY
                ? "\\x" + HexFormat.of().formatHex((byte[]) value)
                : String.valueOf(value);
    }

    private static long tierKeyOf(Object v) {
        if (v instanceof Long l) {
            return l;
        }
        throw new IllegalStateException("tier-key must decode to a long, got "
                + (v == null ? "NULL" : v.getClass().getSimpleName()));
    }

    private static List<Column> columnsOf(ResultSet rs) throws Exception {
        var meta = rs.getMetaData();
        List<Column> out = new ArrayList<>(meta.getColumnCount());
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            out.add(PgValues.column(meta.getColumnName(i), meta.getColumnTypeName(i),
                    meta.getPrecision(i), meta.getScale(i)));
        }
        return out;
    }

    private static int indexOf(List<Column> columns, String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(name)) {
                return i;
            }
        }
        throw new IllegalStateException("column '" + name + "' missing from the copy read");
    }

    private static List<String> readLastPk(DataSource ds, TableId table) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT jsonb_array_elements_text(last_pk) FROM modak.copy_progress "
                        + "WHERE table_id = ? AND last_pk IS NOT NULL")) {
            ps.setLong(1, table.oid());
            try (ResultSet rs = ps.executeQuery()) {
                List<String> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(rs.getString(1));
                }
                return out.isEmpty() ? null : out;
            }
        }
    }

    private static void journal(DataSource ds, TableId table, List<String> lastPk)
            throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "UPDATE modak.copy_progress SET last_pk = ?::jsonb, "
                        + "chunks_done = chunks_done + 1, updated_at = now() "
                        + "WHERE table_id = ?")) {
            ps.setString(1, jsonArray(lastPk));
            ps.setLong(2, table.oid());
            ps.executeUpdate();
        }
    }

    private static String jsonArray(List<String> values) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"');
            String s = values.get(i);
            for (int j = 0; j < s.length(); j++) {
                char ch = s.charAt(j);
                switch (ch) {
                    case '"' -> sb.append("\\\"");
                    case '\\' -> sb.append("\\\\");
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    default -> {
                        if (ch < 0x20) {
                            sb.append(String.format("\\u%04x", (int) ch));
                        } else {
                            sb.append(ch);
                        }
                    }
                }
            }
            sb.append('"');
        }
        return sb.append(']').toString();
    }

    private static String ident(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }
}
