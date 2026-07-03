# Concepts

Five ideas carry the whole system. Everything else in these docs uses them
without re-explaining.

## Tier key

Every registered table names one immutable bigint column as its tier key: the
axis along which data ages, such as an event time, a sequence number, or an
epoch. The tier key alone decides where a row lives. It is declared at
registration (`--tier-key`) and never changes for a row.

## Table modes

A **tiered** table is `PARTITION BY RANGE` on the tier key. Postgres keeps only
the recent partitions. The worker moves whole partitions behind the data
high-water mark into Iceberg, then drops them from the heap, so old rows stop
costing Postgres anything. With `--lake-retention N`, lake rows are also
expired once they fall `N` tier-key units behind the cut-line, so the table
carries a bounded total history.

A **mirrored** table is any table with a primary key. Postgres keeps the full
copy and takes plain DML, and a logical-replication pump trails every change
into an Iceberg mirror. With `--heap-retention N`, a mirrored table that is also
range-partitioned sheds heap partitions once the mirror provably holds them.
That gives you full history in the lake and a bounded window in Postgres.

## The cut-line (T)

Per table, a single monotonic value `T`. Rows with `tier_key >= T` live in
Postgres, rows below it in the lake. Tiering advances `T`, and readers use it
to split a query between the tiers with no overlap and no gap. For mirrored
tables the analogous frontier is a WAL position `F`, the highest position whose
changes are committed to the lake.

## The pinned snapshot (S)

Iceberg is versioned. Every commit produces a new snapshot. The catalog stores,
next to `T`, the lake snapshot `S` that is consistent with it. The two advance
together, atomically, in one Postgres transaction. A query pins `(T, S)` for
its duration (a row in `modak.read_pins`), so compaction and snapshot expiry
can never mutate data out from under a running read. Pins are
transaction-scoped and carry an expiry, so a crashed client cannot stall the
system forever.

## The delta

Corrections to rows that already moved cold do not rewrite Iceberg on the write
path. They land in `modak.delta`, a sparse PK-keyed overlay of upserts and
tombstones, and every read merges it over the pinned snapshot (newest wins).
A background compaction periodically folds the delta into Iceberg as equality
deletes plus data files and clears the folded rows, version-guarded so a row
corrected again mid-fold survives.

## How a read works

Put together: a query over a registered table is rewritten (by the planner
hook, or explicitly) into

```
recent (heap, tier_key >= T)  ⊕  cold-merge (Iceberg @ S, delta)
```

executed by DuckDB via `pg_duckdb`, under a read pin on `(T, S)`. The result is
one consistent point-in-time view of the whole table, no matter what tiering,
mirroring, or compaction are doing concurrently. Details and knobs:
[Reading](../guides/reading.md).
