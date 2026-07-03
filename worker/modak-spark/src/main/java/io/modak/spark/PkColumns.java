package io.modak.spark;

import io.modak.common.PkCodec;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 * The canonical pk text encoding as a Spark column, matching {@link PkCodec}
 * byte for byte. Single-column keys are a plain string cast, composite keys
 * go through a UDF over the codec.
 */
final class PkColumns {

    private PkColumns() {}

    static Column expression(List<String> pkCols, Dataset<Row> ds) {
        if (pkCols.size() == 1) {
            return ds.col(pkCols.get(0)).cast("string");
        }
        UserDefinedFunction encode = functions.udf(
                (UDF1<scala.collection.Seq<String>, String>) seq -> {
                    List<String> parts = new ArrayList<>(seq.size());
                    for (int i = 0; i < seq.size(); i++) {
                        parts.add(seq.apply(i));
                    }
                    return PkCodec.encode(parts);
                }, DataTypes.StringType);
        Column[] parts = pkCols.stream()
                .map(c -> ds.col(c).cast("string"))
                .toArray(Column[]::new);
        return encode.apply(functions.array(parts));
    }
}
