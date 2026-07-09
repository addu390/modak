# Spark

`tierdb-spark` gives a Spark job one consistent `Dataset<Row>` over a TierDB table: the hot branch from the Postgres heap, the cold branch from the lake at the pinned snapshot, and the delta overlay merged newest-wins. It also routes inserts and deletes, so a job can write rows without knowing which tier they land in.

It is a plain library, not a custom data source. It composes Spark's JDBC source with the Iceberg Spark runtime and follows the [seam protocol](../reference/seam.md) step for step.

## Dependencies

```xml
<dependency>
    <groupId>io.tierdb</groupId>
    <artifactId>tierdb-spark</artifactId>
    <version>0.1.0</version>
</dependency>
```

The application provides Spark 3.5, `iceberg-spark-runtime-3.5`, and the Postgres JDBC driver. None are bundled.

## Reading

```java
SeamOptions options = SeamOptions.builder()
        .jdbcUrl("jdbc:postgresql://db:5432/app?user=app")
        .table("public.events")
        .build();

try (SeamRead read = TierDBSpark.read(spark, options)) {
    Dataset<Row> events = read.dataframe();
    events.filter("device_id = 42").count();
}
```

The read is pinned: opening it inserts a `tierdb.read_pins` row atomically with the captured cut-line, so lake maintenance holds back snapshot expiry until the read closes. Spark is lazy, so close only after the last action on the dataframe. If the process dies without closing, the pin's `expires_at` (default 15 minutes, `pinTtl` on the builder) reclaims it. A direct table has no snapshot to pin, so its pin only holds back file maintenance.

For the same reason, never run Spark's `expire_snapshots` or `remove_orphan_files` procedures against a TierDB-managed table, they cannot see the pins. See [Lake maintenance](../operations/lake-maintenance.md) for what Spark may safely maintain.

The cold branch loads through the Iceberg Spark runtime, which needs an Iceberg catalog on the session even for path-based tables:

```java
SparkSession spark = SparkSession.builder()
        .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.default_iceberg.type", "hadoop")
        .config("spark.sql.catalog.default_iceberg.warehouse", "s3a://lake/warehouse")
        .getOrCreate();
```

When the lake table cannot be reached by the path stored in the catalog (for example, it resolves through a configured Spark catalog), pass the identifier with `lakeTable("my_catalog.db.events_cold")`.

What the dataframe serves follows the table's [mode](../modes/choosing.md):

| Mode | The read |
|------|----------|
| Tiered | Heap above the cut-line, lake at the pinned snapshot below it, delta merged newest-wins. |
| Mirrored | Plain heap scan, the heap is complete. With heap retention, reads like tiered. |
| Direct | Heap above the cut-line, live lake below it, no delta. A scan that outlives a concurrent cold write can observe its commit. |

A job scanning bulk history on a mirrored table can opt into serving most of it from the lake instead, mirroring the extension's [hybrid mode](../tables/reading.md#mirrored-tables-heap-or-hybrid):

```java
SeamOptions options = SeamOptions.builder()
        .jdbcUrl("jdbc:postgresql://db:5432/app?user=app")
        .table("public.readings")
        .hybrid(true)                        // split the scan at the seam
        .hybridLag(3_600)                    // keep the last hour on the heap side
        .mirrorWait(Duration.ofSeconds(10))  // bounded wait for the mirror frontier
        .build();
```

The capture waits (bounded by `mirrorWait`, default 5s) for the mirror frontier to pass the current WAL position, proving the lake holds everything committed so far, then splits the union at `max(tier_key) - hybridLag`. If the frontier lags past the wait, the read silently falls back to the heap, which is always correct. On tiered tables and mirrored tables with retention the option is ignored, those always read two-tier at the stored cut-line.

## Writing

```java
TierDBSpark.write(batch, options);
```

Rows at or above the cut-line are appended to the Postgres heap. Rows below it take the mode's cold path:

| Mode | The cold leg |
|------|--------------|
| Tiered | `op = 0` upserts in `tierdb.delta`, versioned from the shared sequence, visible to every seam reader immediately and folded into the lake by the worker. Spark never writes Iceberg. |
| Mirrored | Everything is a heap append, the cut-line sits at the minimum. Heap retention raises it to the drop boundary, and writes into dropped history take the delta path. |
| Direct | Executors write the merge files in parallel, the driver commits them as one atomic snapshot under the shared per-table advisory lock (concurrent Iceberg commits cannot rebase). No delta. |

A batch containing rows below the table's retention line fails before anything is written: those rows have been expired from the lake, so nothing could ever fold them back.

This is an insert path sized for stragglers, not volume. Bulk historical loads belong on the [bulk ingestion](../ingestion/bulk-ingestion.md) path, which commits staged Parquet or raw records straight to the lake without touching the delta.

## Deleting

```java
TierDBSpark.delete(keys, options);
```

The dataset carries the pk columns plus the tier-key column, explicit because a cold target has no heap row to look it up from. Hot keys delete from the heap over JDBC. Cold keys follow the same split as writes: `op = 1` tombstones in `tierdb.delta`, which hide the row immediately and fold into the lake as equality deletes, or on a direct table equality deletes committed straight to the lake under the same lock. Keys below the retention line are rejected like writes.
