package io.modak.spark;

import io.modak.connector.SeamClient;
import io.modak.connector.SeamOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Spark consumer of the Modak seam protocol (docs/reference/seam.md): pinned
 * two-tier reads and tier-routed writes over a Modak table.
 */
public final class ModakSpark {

    private ModakSpark() {}

    /** Opens a pinned, consistent two-tier read. Close after the last action. */
    public static SeamRead read(SparkSession spark, SeamOptions options) {
        return new SeamRead(spark, options, SeamClient.capture(options, true));
    }

    /**
     * Inserts rows without the caller knowing which tier they land in: hot rows
     * to the heap, cold rows to the delta (upsert, newest version wins).
     */
    public static void write(Dataset<Row> rows, SeamOptions options) {
        SeamWriter.write(rows, options);
    }

    /**
     * Deletes by key without the caller knowing which tier holds the row: hot
     * keys delete from the heap, cold keys become delta tombstones. The dataset
     * carries the pk columns plus the tier-key column.
     */
    public static void delete(Dataset<Row> keys, SeamOptions options) {
        SeamDeleter.delete(keys, options);
    }
}
