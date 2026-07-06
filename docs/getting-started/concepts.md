# Concepts

Five ideas carry the whole system. Everything else in these docs uses them without re-explaining.

## Tier key

Every registered table names one column as its tier key: the axis along which data ages, such as an event time, a sequence number, or an epoch. The column can be `bigint` (or any integer), `timestamptz`, `timestamp`, or `date`. Internally every type maps onto one canonical 64-bit axis through an order-preserving codec (microseconds since the epoch for timestamps, days for dates), so the catalog, the protocol, and the routing logic are the same for all of them. The tier key alone decides where a row lives. It is declared at registration (`--tier-key`, the type is detected) and rarely changes for a row. When an update does change it, TierDB moves the row to where the new value says it belongs.

## Table modes

Four variants, one axis: how much of the table Postgres holds.

A **tiered** table is `PARTITION BY RANGE` on the tier key. Postgres keeps only the recent partitions. The worker moves whole partitions behind the data high-water mark into Iceberg, then drops them from the heap, so old rows stop costing Postgres anything. Each tier holds its own slice, and the lake is the only copy of old rows. With `--lake-retention N`, lake rows are also expired once they fall `N` tier-key units behind the cut-line, so the table carries a bounded total history. Corrections to rows that already moved cold land in `tierdb.delta` instead of rewriting Iceberg.

A **tiered keep-heap** table (`--keep-heap`) tiers the same way, minus the drop: partitions are copied into Iceberg in batches and the cut-line advances, but the heap keeps its full copy and stays writable with plain DML everywhere. A row trigger on tiered partitions mirrors every change into `tierdb.delta`, so seam reads and the lake fold see it. Full history in both stores without a replication slot, at the cost of Postgres never shrinking.

A **fully mirrored** table is any table with a primary key. Postgres keeps the whole copy and takes plain DML, including writes into old data, and a logical-replication pump trails every change into an Iceberg mirror. Nothing routes and nothing needs the delta. The lake is a trailing full copy for analytics engines. Reads default to the plain heap, with an opt-in [hybrid mode](../tables/reading.md#mirrored-tables-heap-or-hybrid) that serves the bulk of a scan from the lake once the mirror provably covers it.

A **mirrored table with heap retention** (`--heap-retention N`) is the middle ground: mirrored, but also range-partitioned, so heap partitions are dropped once they fall `N` units behind the high-water mark and the mirror provably holds them. Full history in the lake, a bounded window in Postgres, and reads split at the seam like a tiered table. Writes below the dropped window take the delta path too: the pump folds them into the mirror between replication batches, so old data stays correctable after its heap rows are gone.

## The cut-line (T)

Per table, a single monotonic value `T`. Rows with `tier_key >= T` live in Postgres, rows below it in the lake. Tiering advances `T`, and readers use it to split a query between the tiers with no overlap and no gap. For mirrored tables the analogous frontier is a WAL position `F`, the highest position whose changes are committed to the lake.

## The pinned snapshot (S)

Iceberg is versioned. Every commit produces a new snapshot. The catalog stores, next to `T`, the lake snapshot `S` that is consistent with it. The two advance together, atomically, in one Postgres transaction. A query pins `(T, S)` for its duration (a row in `tierdb.read_pins`), so compaction and snapshot expiry can never mutate data out from under a running read. Pins are transaction-scoped and carry an expiry, so a crashed client cannot stall the system forever.

## The delta

Corrections to rows the heap no longer holds do not rewrite Iceberg on the write path. They land in `tierdb.delta`, a sparse PK-keyed overlay of upserts and tombstones, and every read merges it over the pinned snapshot (newest wins). A background fold periodically turns the delta into equality deletes plus data files in Iceberg and clears the folded rows, version-guarded so a row corrected again mid-fold survives. On tiered tables the fold is a worker cycle, on mirrored tables with heap retention the mirror pump folds between replication batches. Fully mirrored tables never need the delta, since their corrections are plain heap DML that the pump replays. The delta is sized for corrections, not volume: bulk historical loads go through [bulk ingestion](../ingestion/bulk-ingestion.md) instead.

## How a read works

Put together: a query over a registered table is rewritten (by the planner hook, or explicitly) into

```
recent (heap, tier_key >= T)  ⊕  cold-merge (Iceberg @ S, delta)
```

executed by DuckDB via `pg_duckdb`, under a read pin on `(T, S)`. The result is one consistent point-in-time view of the whole table, no matter what tiering, mirroring, or compaction are doing concurrently. Details and knobs: [Reading](../tables/reading.md).
