# CLI

Both jars expose the same commands (`modak-console.jar` is a superset that adds
the web console to `run`). All wiring comes from
[environment variables](configuration.md).

```
modak-worker [run]
modak-worker register   --table <schema.table> --pk <col>[,<col>...] --tier-key <col>
                        [--mode tiered|mirrored] [--heap-retention <n>] [--lake-retention <n>]
                        [--chunk-rows <n>] [--partition-width <n>]
modak-worker unregister --table <schema.table> [--drop-lake]
modak-worker verify     --table <schema.table>
```

## `run` (default)

Hosts the daemon: leader campaign, tiering, mirror pumps, compaction, status
sweep, lake maintenance. Runs until killed. Multiple instances against the
same database form an HA group where exactly one leads.

## `register`

Onboards a table. See [Registering tables](../guides/registering-tables.md).

| Flag | Meaning |
|------|---------|
| `--table` | `schema.table` of an existing table |
| `--pk` | Primary key column(s), comma-separated for composite keys |
| `--tier-key` | The immutable bigint aging column |
| `--mode` | `tiered` (default) or `mirrored` |
| `--heap-retention` | Mirrored only: drop heap partitions this many tier-key units behind the high-water mark |
| `--lake-retention` | Tiered only: expire lake rows this many tier-key units behind the cut-line. Needs a partition width. Omit to keep everything |
| `--chunk-rows` | Mirrored only: initial-copy chunk size (default 50000) |
| `--partition-width` | Iceberg partition band width. `0` = unpartitioned. Tiered tables infer it from the first range partition |

Re-running `register` is safe: a completed registration is a no-op, an
interrupted mirrored initial copy resumes from its journal.

## `unregister`

Offboards a table: catalog rows (cascade), replication slot, publication, and
`REPLICA IDENTITY` reset. `--drop-lake` also purges the Iceberg table. Without
it the lake table survives, which for tiered tables is the only copy of
reclaimed rows.

## `verify`

Heap-vs-lake audit that exits non-zero on mismatch. See
[Operations](../guides/operations.md#verify).

## Exit codes

`0` success, `1` failure (verify mismatch, fatal error), `2` usage error.
