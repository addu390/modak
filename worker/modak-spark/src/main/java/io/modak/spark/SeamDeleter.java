package io.modak.spark;

import io.modak.connector.SeamClient;
import io.modak.connector.SeamOptions;
import io.modak.connector.SeamState;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Routed deletes: keys at or above the cut-line delete from the heap, keys
 * below it become {@code op = 1} tombstones for the worker to fold into the
 * lake, and keys below the retention line are rejected (expired from the lake).
 * The dataset carries the pk columns plus the tier-key column, explicit because
 * a cold target has no heap row to look it up from.
 */
final class SeamDeleter {

    private SeamDeleter() {}

    static void delete(Dataset<Row> keys, SeamOptions options) {
        SeamState state = SeamClient.capture(options, false);

        Column tierKey = keys.col(state.tierKeyCol());
        Dataset<Row> cold = keys.filter(tierKey.lt(state.tierKeyHi()));
        Long line = state.retentionLine();
        if (line != null && !cold.filter(tierKey.lt(line)).isEmpty()) {
            throw new IllegalStateException("delete on " + options.qualifiedName()
                    + " targets rows below the retention line " + line
                    + ", rows this old have been expired from the lake");
        }

        List<String> pkCols = state.primaryKeyCols();
        Dataset<Row> hot = keys.filter(tierKey.geq(state.tierKeyHi()))
                .select(pkCols.stream()
                        .map(c -> keys.col(c).cast("string"))
                        .toArray(Column[]::new));
        hot.foreachPartition(new HotDelete(options.jdbcUrl(), options.jdbcProperties(),
                options.schemaName(), options.tableName(), pkCols));

        // Tombstone payloads keep the key fields: the equality delete needs typed values.
        Column[] pkFields = pkCols.stream().map(cold::col).toArray(Column[]::new);
        Dataset<Row> encoded = cold.select(
                PkColumns.expression(pkCols, cold).as("pk"),
                cold.col(state.tierKeyCol()).cast("long").as("tier_key"),
                functions.to_json(functions.struct(pkFields)).as("payload"));
        encoded.foreachPartition(new DeltaTombstone(
                options.jdbcUrl(), options.jdbcProperties(), state.tableId()));
    }

    private static final class HotDelete implements ForeachPartitionFunction<Row> {

        private static final int BATCH = 500;

        private final String url;
        private final Properties properties;
        private final String sql;
        private final int pkCount;

        HotDelete(String url, Properties properties, String schema, String table,
                List<String> pkCols) {
            this.url = url;
            this.properties = properties;
            this.pkCount = pkCols.size();
            StringBuilder where = new StringBuilder();
            for (int i = 0; i < pkCols.size(); i++) {
                if (i > 0) {
                    where.append(" AND ");
                }
                where.append(ident(pkCols.get(i))).append("::text = ?");
            }
            this.sql = "DELETE FROM " + ident(schema) + "." + ident(table)
                    + " WHERE " + where;
        }

        @Override
        public void call(Iterator<Row> rows) throws Exception {
            if (!rows.hasNext()) {
                return;
            }
            try (Connection c = DriverManager.getConnection(url, properties)) {
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    int pending = 0;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        for (int i = 0; i < pkCount; i++) {
                            ps.setString(i + 1, row.getString(i));
                        }
                        ps.addBatch();
                        if (++pending == BATCH) {
                            ps.executeBatch();
                            pending = 0;
                        }
                    }
                    if (pending > 0) {
                        ps.executeBatch();
                    }
                }
                c.commit();
            }
        }

        private static String ident(String name) {
            return "\"" + name.replace("\"", "\"\"") + "\"";
        }
    }

    private static final class DeltaTombstone implements ForeachPartitionFunction<Row> {

        private static final String SQL = """
                INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload)
                VALUES (?, ?, 1, ?, nextval('modak.delta_version'), ?::jsonb)
                ON CONFLICT (table_id, pk) DO UPDATE
                   SET op = 1, tier_key = excluded.tier_key,
                       old_tier_key = nullif(
                           coalesce(modak.delta.old_tier_key, modak.delta.tier_key),
                           excluded.tier_key),
                       version = excluded.version,
                       payload = excluded.payload, updated_at = now()
                 WHERE modak.delta.version < excluded.version
                """;

        private static final int BATCH = 500;

        private final String url;
        private final Properties properties;
        private final long tableId;

        DeltaTombstone(String url, Properties properties, long tableId) {
            this.url = url;
            this.properties = properties;
            this.tableId = tableId;
        }

        @Override
        public void call(Iterator<Row> rows) throws Exception {
            if (!rows.hasNext()) {
                return;
            }
            try (Connection c = DriverManager.getConnection(url, properties)) {
                c.setAutoCommit(false);
                try (PreparedStatement ps = c.prepareStatement(SQL)) {
                    int pending = 0;
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        ps.setLong(1, tableId);
                        ps.setString(2, row.getString(0));
                        ps.setLong(3, row.getLong(1));
                        ps.setString(4, row.getString(2));
                        ps.addBatch();
                        if (++pending == BATCH) {
                            ps.executeBatch();
                            pending = 0;
                        }
                    }
                    if (pending > 0) {
                        ps.executeBatch();
                    }
                }
                c.commit();
            }
        }
    }
}
