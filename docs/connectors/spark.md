# Spark

`modak-spark` gives a Spark job one consistent `Dataset<Row>` over a Modak
table: the hot branch from the Postgres heap, the cold branch from the lake
at the pinned snapshot, and the delta overlay merged newest-wins. It also
routes inserts, so a job can write rows without knowing which tier they land
in.

It is a plain library, not a custom data source. It composes Spark's JDBC
source with the Iceberg Spark runtime and follows the
[seam protocol](../reference/seam.md) step for step.

## Dependencies

```xml
<dependency>
    <groupId>io.modak</groupId>
    <artifactId>modak-spark</artifactId>
    <version>0.1.0</version>
</dependency>
```

The application provides Spark 3.5, `iceberg-spark-runtime-3.5`, and the
Postgres JDBC driver. None are bundled.

## Reading

```java
SeamOptions options = SeamOptions.builder()
        .jdbcUrl("jdbc:postgresql://db:5432/app?user=app")
        .table("public.events")
        .build();

try (SeamRead read = ModakSpark.read(spark, options)) {
    Dataset<Row> events = read.dataframe();
    events.filter("device_id = 42").count();
}
```

The read is pinned: opening it inserts a `modak.read_pins` row atomically with
the captured cut-line, so lake maintenance holds back snapshot expiry until
the read closes. Spark is lazy, so close only after the last action on the
dataframe. If the process dies without closing, the pin's `expires_at`
(default 15 minutes, `pinTtl` on the builder) reclaims it.

The cold branch loads through the Iceberg Spark runtime, which needs an
Iceberg catalog on the session even for path-based tables:

```java
SparkSession spark = SparkSession.builder()
        .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.default_iceberg.type", "hadoop")
        .config("spark.sql.catalog.default_iceberg.warehouse", "s3a://lake/warehouse")
        .getOrCreate();
```

When the lake table cannot be reached by the path stored in the catalog (for
example, it resolves through a configured Spark catalog), pass the identifier
with `lakeTable("my_catalog.db.events_cold")`.

Mirrored tables without retention read as a plain heap scan, since the heap is
complete.

## Writing

```java
ModakSpark.write(batch, options);
```

Rows at or above the cut-line are appended to the Postgres heap. Rows below it
become `op = 0` upserts in `modak.delta`, versioned from the shared sequence,
visible to every seam reader immediately and folded into the lake by
compaction. The worker owns lake commits, so Spark never writes Iceberg
directly.

This is an insert path. Deletes and bulk cold backfills are not supported
through it.
