# CLI

Both jars expose the same commands (`tierdb-console.jar` is a superset that adds the web console to `run`). All wiring comes from [environment variables](configuration.md).

```
tierdb-worker [run]
tierdb-worker register   --table <schema.table> --pk <col>[,<col>...] --tier-key <col>
                        [--mode tiered|mirrored] [--heap-retention <n>] [--lake-retention <n>]
                        [--chunk-rows <n>] [--partition-width <n>] [--profile <name>]
tierdb-worker unregister --table <schema.table> [--drop-lake]
tierdb-worker verify     --table <schema.table>
tierdb-worker ingest     --table <schema.table> [--file <parquet>...] [--jsonl <file>]
tierdb-worker policy     --table <schema.table> [--set <key=value>...] [--unset <key>...] [--reset]
tierdb-worker maintain   --table <schema.table> [--no-wait]
tierdb-worker profile    list
tierdb-worker profile    create --name <name> --warehouse <root> [--format <plugin>]
                               [--config <key=value;...>] [--credentials <ref>] [--default]
```

## `run` (default)

Hosts the daemon: leader campaign, tiering, mirror pumps, compaction, status sweep, lake maintenance. Runs until killed. Multiple instances against the same database form an HA group where exactly one leads.

## `register`

Onboards a table. See [Registering tables](../tables/registering-tables.md).

| Flag | Meaning |
|------|---------|
| `--table` | `schema.table` of an existing table |
| `--pk` | Primary key column(s), comma-separated for composite keys |
| `--tier-key` | The aging column: `bigint` (or any integer), `timestamptz`, `timestamp`, or `date`. The type is detected |
| `--mode` | `tiered` (default) or `mirrored` |
| `--heap-retention` | Mirrored only: drop heap partitions this far behind the high-water mark. Temporal keys take durations (`7d`, `12h`), integer keys take numbers |
| `--lake-retention` | Tiered only: expire lake rows this far behind the cut-line, same units as `--heap-retention`. Needs a partition width. Omit to keep everything |
| `--keep-heap` | Tiered only: never drop heap partitions, a trigger mirrors their DML into the delta. Excludes `--lake-retention` |
| `--chunk-rows` | Mirrored only: initial-copy chunk size (default 50000) |
| `--partition-width` | Lake partition band width for integer keys (`0` = unpartitioned), inferred from the first range partition on tiered tables |
| `--lake-partition` | Temporal keys only: lake layout `hour`, `day` (default), `month`, `year`, or `none` |
| `--profile` | Storage profile the table's lake lives on. Omit for the default profile. See [Storage profiles](../tables/storage-profiles.md) |

Re-running `register` is safe: a completed registration is a no-op, an interrupted mirrored initial copy resumes from its journal.

## `unregister`

Offboards a table: catalog rows (cascade), replication slot, publication, and `REPLICA IDENTITY` reset. `--drop-lake` also purges the Iceberg table. Without it the lake table survives, which for tiered tables is the only copy of reclaimed rows.

## `verify`

Heap-vs-lake audit that exits non-zero on mismatch. See [Operations](../operations/day-2.md#verify).

## `ingest`

Commits rows straight into a table's lake as one atomic upsert, bypassing `tierdb.delta`. Input is staged Parquet (`--file`, adopted by reference) or JSONL records (`--jsonl`, the worker writes the Parquet). Applies to tiered tables and mirrored tables with heap retention. Every row must be cold: below the cut-line, at or above the retention line. See [Bulk ingestion](../ingestion/bulk-ingestion.md).

## `policy`

Views or edits a table's maintenance policy, the per-table overrides layered over the worker's defaults. With no edit flags it prints every setting maintenance will run with and where each comes from. Keys belong to the lake format, see [Lake maintenance](../operations/lake-maintenance.md).

```bash
tierdb-worker policy --table public.events
tierdb-worker policy --table public.events --set snapshot_retention_hours=6
tierdb-worker policy --table public.events --reset
```

## `maintain`

Requests an out-of-schedule maintenance pass by filing a row in `tierdb.maintenance_requests`. The leader claims it on its next cycle, the command waits for the journal entry and prints what the pass did (`--no-wait` files and returns). See [Lake maintenance](../operations/lake-maintenance.md#forcing-a-pass).

## `profile`

Lists or creates storage profiles, the named warehouse bindings tables register against. `create` takes `--name`, `--warehouse`, and optionally `--format`, `--config` (semicolon-separated `key=value` overrides), `--credentials` (a reference resolved from the worker's environment, never a key), and `--default`. See [Storage profiles](../tables/storage-profiles.md).

```bash
tierdb-worker profile create --name analytics \
    --warehouse s3://analytics-lake/warehouse --credentials analytics
tierdb-worker profile list
```

## Exit codes

`0` success, `1` failure (verify mismatch, fatal error), `2` usage error.
