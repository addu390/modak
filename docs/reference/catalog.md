# Catalog schema

The `modak.*` tables are the coordination contract between the extension and
the workers. They are the only channel, there is no RPC. Every atomic handoff
is a plain Postgres transaction. The schema lives in `sql/catalog.sql` and is
created and migrated automatically by the worker at startup
(`modak.schema_meta` stamps the installed version).

All columns stay executor-portable (plain ints, text, jsonb) because
`pg_duckdb` may push read-path queries down to DuckDB.

## `modak.tables`

Registered logical tables. One row per registration.

| Column | Meaning |
|--------|---------|
| `table_id` | The user table's OID, as bigint |
| `schema_name`, `table_name` | The registered relation |
| `primary_key_cols` | Merge key (array; composite keys supported) |
| `tier_key_col` | The immutable aging column |
| `partition_scheme` | Lake partition layout, e.g. `{"width": 3600}` |
| `lake_format` | Lake plugin id (`iceberg`) |
| `lake_table_ref` | The format's name for the cold table (path or catalog identifier) |
| `lake_props` | Opaque per-format state, e.g. Iceberg's `metadata_location` |
| `mode` | `tiered` or `mirrored` |
| `publication_name`, `slot_name` | Mirrored: CDC plumbing |
| `heap_retention_lag` | Mirrored: heap retention window. `NULL` = keep all |
| `lake_retention_lag` | Tiered: expire lake rows this far behind the cut-line. `NULL` = keep forever |

## `modak.cutline`

The seam, per table, always advanced together in one transaction.

| Column | Meaning |
|--------|---------|
| `tier_key_hi` | `T`: rows with `tier_key >= T` live in Postgres |
| `lake_snapshot_id` | `S`: pinned cold-store version consistent with `T` |
| `replicated_lsn` | `F`: the mirror frontier (WAL position). `NULL` for tiered tables |
| `retention_line` | `R`: lake rows with `tier_key < R` are expired. `NULL` = nothing expired yet |

Connectors reading the seam should treat `retention_line` as the floor of the
table: rows below it exist in old lake snapshots but not in the current one,
and writes targeting them are rejected by the extension.

## `modak.partitions`

Partition lifecycle map: `hot → sealing → tiering → tiered → dropped`, with
tier-key bounds per partition.

## `modak.delta`

The correction overlay for cold rows, merged on read, folded by compaction.

| Column | Meaning |
|--------|---------|
| `pk` | Canonical text PK (composite keys joined with `chr(31)`, escaped) |
| `op` | `0` = upsert, `1` = tombstone |
| `tier_key` | Of the target cold row (`< T`) |
| `version` | Newest-wins ordering (sequence-assigned), guards the fold clear |
| `payload` | Row image. Tombstones keep the pk fields |

## `modak.read_pins`

Active read pins. The oldest pin is the reclaim and compaction horizon. Pins
are transaction-scoped rows with an `expires_at` bound.

## `modak.tiering_log`

Idempotency + crash-resume journal for tiering, compaction, and maintenance
operations (`op_id`, `op_kind`, `phase`, snapshot, details).

## `modak.copy_progress`

In-flight mirrored initial copies: the slot's consistent point, the last
copied PK, and chunks done. The row exists only while the copy runs, and a
re-run of `register` resumes from it.

## `modak.status`

The operational view, one row per table for humans and dashboards:

```sql
SELECT * FROM modak.status;
--  table_id | schema_name | table_name | mode | cutline_t | cutline_s |
--  mirror_frontier | retention_line | cutline_updated_at | delta_backlog |
--  read_pins | copying | partition_states
```
