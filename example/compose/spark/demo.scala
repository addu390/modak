import io.tierdb.connector.seam.SeamOptions
import io.tierdb.spark.TierDBSpark

val options = SeamOptions.builder()
    .jdbcUrl(sys.env("TIERDB_PG_URL"))
    .jdbcProperty("user", sys.env("TIERDB_PG_USER"))
    .jdbcProperty("password", sys.env("TIERDB_PG_PASSWORD"))
    .table(sys.env("TIERDB_TABLE"))
    .build()

val read = TierDBSpark.read(spark, options)
val rows = read.dataframe().collect().map(_.mkString(",")).sorted
rows.foreach(r => println("ROW " + r))
println("COUNT " + rows.length)
read.close()

System.exit(0)
