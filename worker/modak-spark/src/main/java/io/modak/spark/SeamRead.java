package io.modak.spark;

import io.modak.connector.SeamClient;
import io.modak.connector.SeamOptions;
import io.modak.connector.SeamState;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * One pinned two-tier read: the heap over JDBC, the lake at the pinned
 * snapshot, and the delta overlay merged newest-wins, unioned into one
 * dataframe. Spark is lazy, so close only after the last action. The pin's
 * {@code expires_at} covers a consumer that dies without closing.
 */
public final class SeamRead implements AutoCloseable {

    private final SparkSession spark;
    private final SeamOptions options;
    private final SeamState state;
    private Dataset<Row> dataframe;
    private boolean closed;

    SeamRead(SparkSession spark, SeamOptions options, SeamState state) {
        this.spark = spark;
        this.options = options;
        this.state = state;
    }

    public Dataset<Row> dataframe() {
        if (dataframe == null) {
            dataframe = build();
        }
        return dataframe;
    }

    @Override
    public void close() {
        if (!closed && state.pinId() != null) {
            SeamClient.release(options, state.pinId());
        }
        closed = true;
    }

    private Dataset<Row> build() {
        if (state.heapIsComplete()) {
            return jdbc("(SELECT * FROM " + options.qualifiedName() + ") modak_hot");
        }

        Dataset<Row> hot = jdbc("(SELECT * FROM " + options.qualifiedName()
                + " WHERE " + state.tierKeyCol() + " >= " + state.tierKeyHi() + ") modak_hot");
        if (state.snapshotId() == null) {
            return hot;
        }

        Dataset<Row> cold = coldBranch();

        Dataset<Row> delta = jdbc("(SELECT pk AS __modak_pk, op AS __modak_op,"
                + " payload::text AS __modak_payload"
                + " FROM modak.delta WHERE table_id = " + state.tableId() + ") modak_delta");

        Column coldPk = PkColumns.expression(state.primaryKeyCols(), cold);
        Dataset<Row> survivors = cold.join(delta,
                coldPk.equalTo(delta.col("__modak_pk")), "left_anti");
        Dataset<Row> upserts = delta
                .filter(delta.col("__modak_op").equalTo(0))
                .select(functions.from_json(delta.col("__modak_payload"), cold.schema())
                        .as("__modak_row"))
                .select("__modak_row.*");

        return hot.unionByName(survivors.unionByName(upserts));
    }

    private Dataset<Row> coldBranch() {
        if (!"iceberg".equals(state.lakeFormat())) {
            throw new UnsupportedOperationException("modak-spark cannot read lake format '"
                    + state.lakeFormat() + "' yet (supported: iceberg)");
        }
        return spark.read()
                .format("iceberg")
                .option("snapshot-id", state.snapshotId())
                .load(options.lakeTable() != null ? options.lakeTable()
                        : state.lakeTableRef());
    }

    private Dataset<Row> jdbc(String table) {
        return spark.read().jdbc(options.jdbcUrl(), table, options.jdbcProperties());
    }
}
