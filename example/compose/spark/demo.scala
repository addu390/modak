import io.modak.connector.seam.SeamOptions
import io.modak.spark.ModakSpark

val options = SeamOptions.builder()
    .jdbcUrl(sys.env("MODAK_PG_URL"))
    .jdbcProperty("user", sys.env("MODAK_PG_USER"))
    .jdbcProperty("password", sys.env("MODAK_PG_PASSWORD"))
    .table(sys.env("MODAK_TABLE"))
    .build()

val read = ModakSpark.read(spark, options)
val rows = read.dataframe().collect().map(_.mkString(",")).sorted
rows.foreach(r => println("ROW " + r))
println("COUNT " + rows.length)
read.close()

System.exit(0)
