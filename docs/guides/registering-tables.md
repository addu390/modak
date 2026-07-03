# Registering tables

Registration is the onboarding step. It records the table in the `modak.*`
catalog, creates its Iceberg counterpart, and (for mirrored tables) sets up
CDC. It runs through the worker binary's CLI, the same jar as the daemon:

```bash
modak-worker register --table <schema.table> --pk <col>[,<col>...] --tier-key <col> \
                      [--mode tiered|mirrored] [--heap-retention <n>] [--lake-retention <n>] \
                      [--chunk-rows <n>] [--partition-width <n>]
```

## Choosing a mode

| Mode | Heap keeps | Iceberg keeps | Writes go |
|------|-----------|---------------|-----------|
| `tiered` (default) | recent partitions only | everything below the cut-line | plain DML for hot rows, `modak_upsert`/`modak_delete` for corrections to cold rows |
| `mirrored` | everything | everything, trailing by CDC | plain DML, always, no Modak API |
| `mirrored --heap-retention N` | rows above the retention line | everything | plain DML, always |

Use `tiered` for append-mostly, time-series data where old rows should stop
occupying Postgres. It requires `PARTITION BY RANGE` on a bigint tier key, and
partitions are dropped once safely in Iceberg.

Use `mirrored` for reference and dimension tables that must stay fully
queryable and writable in Postgres (any `UPDATE` or `DELETE`, no routing rules)
but should also be in the lake for analytics. Any table with a primary key
qualifies.

Use `mirrored --heap-retention N` when the table is range-partitioned on the
tier key and Postgres should keep only the recent window while Iceberg keeps
all of it. Partitions whose changes the mirror provably holds are dropped once
they fall `N` tier-key units behind the high-water mark. Plain reads span both
tiers automatically, like `tiered`.

Composite primary keys work in both modes: `--pk tenant_id,device_id`.

## The full lifecycle

For a tiered table, data moves through three stages. Recent rows live in the
heap. Once a partition falls `MODAK_TIERING_LAG` behind the high-water mark it
is tiered into Iceberg and the heap partition is dropped. And if the table was
registered with `--lake-retention N`, lake rows that fall `N` tier-key units
behind the cut-line are expired entirely — heap, then lake, then gone.

Retention is pin-aware like every other pass: it never runs while a reader
holds a pin, the boundary is aligned to the partition width so the Iceberg
delete removes whole files without rewriting, and it never passes a partition
whose heap rows still exist. The current boundary is `retention_line` in
`modak.status`. Corrections (`modak_upsert`/`modak_delete`) targeting rows
below the line are rejected, since there is nothing left to correct.

Retention is tiered-only. A mirrored table's heap drop relies on the lake
holding full history, so the two retention flags exclude each other.

## Future partitions

The daemon premakes heap partitions ahead of the write frontier, so inserts
never fail for lack of a partition. Each cycle it keeps at least
`MODAK_PREMAKE_PARTITIONS` (default 2) empty partition widths between the
table's `max(tier_key)` and the top partition bound, inferring the width from
the topmost existing partition. Operators create the first partitions at
`CREATE TABLE` time; the worker takes it from there.

## What tiered registration does

1. Creates the Iceberg table (create-if-absent, so re-running is safe) and
   seeds its `metadata_location` in the catalog, so pinned reads work before
   the first commit.
2. Initializes the cut-line at the oldest partition floor.
3. Mirrors the table's `pg_inherits` children into `modak.partitions`. From
   then on you just `CREATE TABLE ... PARTITION OF` and the worker picks new
   partitions up on its next cycle.

## What mirrored registration does

1. Sets `REPLICA IDENTITY FULL` on the table (deletes and TOAST updates need
   the old row image).
2. Creates a publication and a logical replication slot. The slot's consistent
   point marks where streaming will start.
3. Copies existing rows to Iceberg in PK-ordered chunks (`--chunk-rows`,
   default 50000), each chunk journaled in `modak.copy_progress`.

The copy is fully resumable. If it dies, re-run the same `register` command
and it continues from the last journaled chunk. Streaming then starts at the
slot's consistent point, so rows changed during the copy are healed by the
idempotent fold, with no gap and no duplicates. Until the copy lands the table
has no mirror frontier and the daemon skips it.

## Iceberg partitioning

New Iceberg tables are laid out as `truncate(tier_key, width)`, one lake
partition per width-sized tier-key band, so lake engines and maintenance can
prune by tier key. Tiered tables infer the width from the first Postgres range
partition. Mirrored and unpartitioned tables stay unpartitioned unless you
pass `--partition-width` explicitly (`0` forces unpartitioned). The spec
applies at table creation only. Re-registering does not rewrite an existing
layout.

## Permissions

Registration alters the table, creates a publication, and creates a
replication slot, so run it as a role with table ownership and `REPLICATION`.
See [Production deployment](production.md#roles-and-grants) for the exact
grants.
