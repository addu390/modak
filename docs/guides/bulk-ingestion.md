# Bulk ingestion

`modak.delta` stores one row per correction, as JSONB, in Postgres. That is the
right shape for corrections and stragglers, and the wrong shape for loading two
years of history: millions of rows would pass through the heap only to be
folded back out. Bulk ingestion is the volume path. Rows commit straight into
the lake as one atomic operation, and the delta is never touched. For
continuous micro-batches from a running source, see
[Stream load](stream-load.md), which reuses this machinery per labeled batch.

The semantics are the same as every other cold write in Modak: upsert. A row
whose primary key already exists in the lake replaces it, a fresh row inserts.
You do not declare which case you are in. Loading history and re-loading a
corrected copy of it are the same command. When nothing in the target range
exists yet, the commit is a plain append and costs no read pass.

Applies to tiered tables and to mirrored tables with heap retention, where
rows below the drop boundary live only in the lake. A fully mirrored table's
heap is the source of truth, so bulk data enters it as plain `INSERT`s (or
`COPY`) and the mirror pump carries it to the lake.

## Two ways in

**Staged Parquet.** Write files with the table's schema to a location the
worker can read (the lake warehouse, or any path its filesystem config
resolves), then hand over the paths. The files are adopted by reference, so do
not stage them somewhere you plan to delete.

```
modak-worker ingest --table public.events \
    --file s3a://staging/events/part-000.parquet \
    --file s3a://staging/events/part-001.parquet
```

**Records.** Hand over JSONL (one object per line, keys matching column names)
and the worker writes the Parquet itself, partition-aligned. This is the
classic bulk-insert shape for tens of thousands of rows when you do not want
to produce Parquet.

```
modak-worker ingest --table public.events --jsonl backfill.jsonl
```

Either way the worker validates everything first, commits once, and advances
the pinned snapshot `S` so readers see the rows. The cut-line `T` does not
move. If anything fails validation, nothing commits.

## Validation

Every row must land in the cold window `[R, T)`, checked per file from Parquet
footer statistics or per record before staging:

- **Below the cut-line.** Rows at or above `T` belong to the heap. Insert them
  through Postgres or a connector.
- **At or above the retention line.** Rows below `R` have been expired and
  cannot come back.
- **Within one partition band.** On a partitioned lake table, a Parquet file
  must not straddle a `truncate(tier_key, width)` boundary. Split files at
  multiples of the width. JSONL staging does this automatically. This keeps
  retention file-aligned.

Parquet files should carry Iceberg field IDs matching the lake schema (writers
built on the Iceberg libraries do this). One batch must not contain the same
primary key twice, since within a single commit there is no "newest" to win.

## Crash behaviour

An ingest journals to `modak.tiering_log` (`op_kind = 'ingest'`) like every
other lake operation. The commit is atomic: a crash before it leaves nothing,
a crash after it leaves the data committed with the snapshot advance pending,
which the next successful pass or lake commit makes visible.
