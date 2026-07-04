package io.modak.spark;

import io.modak.connector.SeamClient;
import io.modak.connector.SeamOptions;
import io.modak.connector.SeamState;
import io.modak.load.DeltaLoader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Iterator;
import java.util.Properties;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;

/**
 * Routed inserts: rows at or above the cut-line go to the heap, rows below it
 * become {@code op = 0} delta rows for the worker to fold into the lake, and
 * rows below the retention line are rejected (expired from the lake). The same
 * split covers every mode: a fully mirrored table keeps T at its minimum so
 * everything is heap, and heap retention raises T to the drop boundary. No pin
 * is needed because T only moves up and the worker owns all lake commits.
 */
final class SeamWriter {

    private SeamWriter() {}

    static void write(Dataset<Row> rows, SeamOptions options) {
        SeamState state = SeamClient.capture(options, false);

        Column tierKey = rows.col(state.tierKeyCol());
        Dataset<Row> cold = rows.filter(tierKey.lt(state.tierKeyHi()));
        Long line = state.retentionLine();
        if (line != null && !cold.filter(tierKey.lt(line)).isEmpty()) {
            throw new IllegalStateException("write to " + options.qualifiedName()
                    + " contains rows below the retention line " + line
                    + ", rows this old have been expired from the lake");
        }

        append(rows.filter(tierKey.geq(state.tierKeyHi())), options);
        Dataset<Row> encoded = cold.select(
                PkColumns.expression(state.primaryKeyCols(), cold).as("pk"),
                cold.col(state.tierKeyCol()).cast("long").as("tier_key"),
                functions.to_json(functions.struct(functions.col("*"))).as("payload"));
        encoded.foreachPartition(new DeltaUpsert(
                options.jdbcUrl(), options.jdbcProperties(), state.tableId()));
    }

    private static void append(Dataset<Row> rows, SeamOptions options) {
        rows.write().mode(SaveMode.Append)
                .jdbc(options.jdbcUrl(), options.qualifiedName(), options.jdbcProperties());
    }

    /** Per-partition bridge into {@link DeltaLoader}, the shared delta upsert. */
    private static final class DeltaUpsert implements ForeachPartitionFunction<Row> {

        private final String url;
        private final Properties properties;
        private final long tableId;

        DeltaUpsert(String url, Properties properties, long tableId) {
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
                DeltaLoader.upsert(c, tableId, new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return rows.hasNext();
                    }

                    @Override
                    public DeltaLoader.Entry next() {
                        Row row = rows.next();
                        return new DeltaLoader.Entry(
                                row.getString(0), row.getLong(1), row.getString(2));
                    }
                });
                c.commit();
            }
        }
    }
}
