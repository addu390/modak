# Registering tables

Registration is the onboarding step. It records the table in the `tierdb.*` catalog, creates its Iceberg counterpart, and (for mirrored tables) sets up CDC. It runs through the worker binary's CLI, the same jar as the daemon:

```bash
tierdb-worker register --table <schema.table> --pk <col>[,<col>...] --tier-key <col> \
                      [--mode tiered|mirrored] [--heap-retention <n>] [--lake-retention <n>] \
                      [--keep-heap] [--chunk-rows <n>] [--partition-width <n>] \
                      [--lake-partition hour|day|month|year|none] [--profile <name>]
```

`--profile` places the table's lake on a named [storage profile](storage-profiles.md) (a different bucket, account, or provider). Omitted, the table lands on the default profile: the worker's own warehouse configuration.

## Picking the mode

Undecided? [Choosing a mode](../modes/choosing.md) walks the decision from the shape of the data, and [The contract](../modes/contract.md) states the full operation and API matrix per mode. In short: `tiered` (the default) for append-mostly time series, `tiered --keep-heap` when the heap should keep its full copy anyway, `mirrored` for entity tables, and `mirrored --heap-retention N` for time series whose lake copy must trail by CDC.

Tiered and heap-retention tables require `PARTITION BY RANGE` on the tier key. Mirrored tables require a primary key, and composite keys work in both modes: `--pk tenant_id,device_id`.

## Tier key types

The tier key can be `bigint` (or any integer), `timestamptz`, `timestamp`, or `date`. The type is detected at registration and stored in the catalog, and everything downstream (partition bounds, DML routing, lake layout, the `tierdb_*` SQL API) works in the column's native type. For temporal keys the lag and width flags take durations: `--heap-retention 7d`, `--lake-retention 90d`, `--partition-width 1d` (`s`, `m`, `h`, `d`). Integer keys keep taking plain numbers.

## The full lifecycle

For a tiered table, data moves through three stages. Recent rows live in the heap. Once a partition falls `TIERDB_TIERING_LAG` behind the high-water mark it is tiered into Iceberg and the heap partition is dropped. And if the table was registered with `--lake-retention N`, lake rows that fall `N` tier-key units behind the cut-line are expired entirely: heap, then lake, then gone.

Retention is pin-aware like every other pass: it never runs while a reader holds a pin, the boundary is aligned to the partition width so the Iceberg delete removes whole files without rewriting, and it never passes a partition whose heap rows still exist. The current boundary is `retention_line` in `tierdb.status`. Corrections (`tierdb_upsert`/`tierdb_delete`) targeting rows below the line are rejected, since there is nothing left to correct.

Retention is tiered-only. A mirrored table's heap drop relies on the lake holding full history, so the two retention flags exclude each other.

## Keep-heap

`--keep-heap` (tiered-only) turns off the drop stage. Partitions still tier into Iceberg and the cut-line still advances, but every heap partition rests at `TIERED` and keeps its rows. When a partition tiers, the worker attaches the extension's cold-mirror trigger to it, so plain DML below the cut-line keeps flowing into `tierdb.delta` and from there into the lake. Because keep-heap means nothing is deleted anywhere, it excludes `--lake-retention`, and `tierdb-worker verify` gains a heap-vs-lake comparison below the cut-line, since the heap stays complete and comparable.

## Future partitions

The daemon premakes heap partitions ahead of the write frontier, so inserts never fail for lack of a partition. Each cycle it keeps at least `TIERDB_PREMAKE_PARTITIONS` (default 2) empty partition widths between the table's `max(tier_key)` and the top partition bound, inferring the width from the topmost existing partition. Operators create the first partitions at `CREATE TABLE` time, and the worker takes it from there.

## What tiered registration does

1. Creates the Iceberg table (create-if-absent, so re-running is safe) and seeds its `metadata_location` in the catalog, so pinned reads work before the first commit.
2. Initializes the cut-line at the oldest partition floor.
3. Mirrors the table's `pg_inherits` children into `tierdb.partitions`. From then on you just `CREATE TABLE ... PARTITION OF` and the worker picks new partitions up on its next cycle.

## What mirrored registration does

1. Sets `REPLICA IDENTITY FULL` on the table (deletes and TOAST updates need the old row image).
2. Creates a publication and a logical replication slot. The slot's consistent point marks where streaming will start.
3. Copies existing rows to Iceberg in PK-ordered chunks (`--chunk-rows`, default 50000), each chunk journaled in `tierdb.copy_progress`.

The copy is fully resumable. If it dies, re-run the same `register` command and it continues from the last journaled chunk. Streaming then starts at the slot's consistent point, so rows changed during the copy are healed by the idempotent fold, with no gap and no duplicates. Until the copy lands the table has no mirror frontier and the daemon skips it.

## Lake partitioning

New lake tables are partitioned on the tier key so lake engines and maintenance can prune by it. Integer keys are laid out as `truncate(tier_key, width)`, one lake partition per width-sized band, with the width inferred from the first Postgres range partition (or `--partition-width`, `0` forces unpartitioned). Temporal keys default to a `day` layout and `--lake-partition hour|day|month|year` overrides it (`none` forces unpartitioned). The spec applies at table creation only. Re-registering does not rewrite an existing layout.

## Permissions

Registration alters the table, creates a publication, and creates a replication slot, so run it as a role with table ownership and `REPLICATION`. See [Production deployment](../operations/production.md#roles-and-grants) for the exact grants.
