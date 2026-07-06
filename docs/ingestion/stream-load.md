# Stream load

Bulk ingestion moves history in one shot. Stream Load is its continuous counterpart: many small labeled batches from an always-on source such as a Kafka consumer, a CDC pipe from another system, or an application flushing events every few seconds. Each batch carries a client-chosen label, and TierDB applies each label exactly once, no matter how many times the client retries it.

It is a library first and a service second. The `tierdb-load` module holds the whole engine: routing, labels, loaders, spooling. The worker hosts that library behind one HTTP endpoint, which is the zero-dependency way in. A Flink or Spark Streaming sink embeds the same library directly and skips HTTP entirely; the guarantees are identical because they are the library's, not the endpoint's.

## Labels are the retry contract

Every batch names a label, and `(table, label)` is unique forever. The first attempt applies the batch and records the outcome. Every later attempt with the same label returns that recorded outcome without touching the table. So a source that delivers at least once (a Kafka consumer that replays a partition after a rebalance, a job that crashes after the request left but before the response arrived) becomes effectively exactly once: derive the label from the source position, e.g. `orders-topic-3-offset-91100`, and retry blindly.

Two rules follow. A label must mean the same rows on every attempt, so never reuse one for different data. And a rejected batch (validation failure) records nothing, so the client may fix the data and retry the same label.

## Where rows land

Each batch is routed row by row against one atomic capture of the table's seam state:

- **Hot rows** (`tier_key >= T`, or everything on a fully mirrored table) are upserted into the Postgres heap via `COPY` into a temp table and one `INSERT ... ON CONFLICT` statement. Visible immediately.
- **A cold trickle** (rows in `[R, T)`, up to the spool threshold, default 1000 per batch) goes to `tierdb.delta` like any correction. Visible immediately, folded into Iceberg by the next sweep.
- **Cold volume** (above the threshold) is written as partition-aligned Parquet and registered as a `staged` label. The worker's adoption pass batches every staged label on the table into a single Iceberg commit and advances the pinned snapshot `S`, at which point the rows are visible.
- **Rows below the retention line `R`** reject the whole batch. Expired data does not come back through a side door.

The heap upsert, the delta rows, the staged-file registration, and the label row all commit in one Postgres transaction. A crash mid-load leaves either everything or nothing; orphaned Parquet from a crash before the commit is never referenced and never adopted. The same rule as everywhere else in TierDB: within one batch a primary key may appear only once.

## The HTTP endpoint

The worker (headless or console) mounts `POST /api/load/{schema}.{table}` when `TIERDB_LOAD_TOKEN` is set; without the token the endpoint does not exist. The body is JSONL, one object per line, keys matching column names. The label rides in a header.

```
curl -sS -X POST http://worker:9090/api/load/public.events \
    -H "X-TierDB-Token: $TIERDB_LOAD_TOKEN" \
    -H "X-TierDB-Label: events-batch-000042" \
    --data-binary $'{"id":101,"event_time":260,"val":"a"}\n{"id":102,"event_time":261,"val":"b"}'
```

The response is the load result:

```json
{"label":"events-batch-000042","state":"committed","hot_rows":2,
 "delta_rows":0,"spooled_rows":0,"staged_files":[],"replay":false}
```

| Field | Meaning |
|---|---|
| `state` | `committed` (all rows visible) or `staged` (cold volume awaiting adoption) |
| `hot_rows` / `delta_rows` / `spooled_rows` | how many rows took each path |
| `replay` | true when this label was already applied and nothing ran |

Status codes: `200` applied or replayed, `400` rejected (bad JSONL, missing label, duplicate PK, rows below `R`; nothing recorded, fix and retry the same label), `401` wrong or missing token, `409` the same label is in flight on another connection right now (retry shortly, one of you wins), `405` not a POST. `Authorization: Bearer <token>` works in place of `X-TierDB-Token`.

On the Docker stack the endpoint shares port 9090 with the console. In production put it behind TLS like any other internal service; the token is a shared secret, not a user system.

## Embedding the library

The endpoint is one thin host. Anything on the JVM can be another:

```java
LoadClient client = new LoadClient(LoadOptions.builder()
        .jdbcUrl("jdbc:postgresql://pg:5432/postgres")
        .credentials("app", secret)
        .table("public.events")
        .build(), lakeStorageResolver);

LoadResult r = client.load(new LoadRequest("orders-3-91100", rows));
```

`LoadClient` talks straight to Postgres (and to the lake's staging location for cold volume); there is no worker in the data path, only in the adoption of staged files afterward. This is the shape a Flink sink takes: labels derived from checkpoint IDs, one `load` per bucket per checkpoint, no HTTP fan-in to scale. Without a lake storage resolver, cold rows all trickle through `tierdb.delta`, which is fine at correction scale.

## Observability

Loads journal like every other lake operation: adoption runs under `op_kind = 'load'` in `tierdb.op_log`, and staged labels are visible in `tierdb.load_labels` (also as `staged_loads` in `tierdb.status`). The worker exports `tierdb_load_total` by table and state, `tierdb_load_rows_total` by path, `tierdb_load_staged_labels`, and `tierdb_load_adoption_lag_seconds`. The console overview shows the staged backlog next to the delta backlog.

Retention will not strand a staged load: the sweep never raises the retention line past the lowest tier key of any staged label, so adoption always completes before the data it carries could expire.

## When to use what

| Shape | Path |
|---|---|
| One-time history move, files already Parquet | `tierdb-worker ingest --file` |
| One-time history move, tens of thousands of records | `tierdb-worker ingest --jsonl` |
| Continuous micro-batches from an app or pipeline | Stream Load over HTTP |
| Continuous micro-batches from Flink/Spark at scale | embed `tierdb-load` |
| A handful of corrections | `tierdb_upsert()` / plain DML |
