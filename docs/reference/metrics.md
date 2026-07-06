# Metrics

Prometheus text format on `/metrics`, served by the headless worker (set `TIERDB_METRICS_PORT`) and by the console binary (on `TIERDB_CONSOLE_PORT`) identically.

## Daemon

| Metric | Meaning |
|--------|---------|
| `tierdb_leader` | 1 on the leading worker, 0 on standbys (fleet sum should be exactly 1) |
| `tierdb_cycle_duration_seconds` | Duration of the last work cycle |
| `tierdb_last_cycle_timestamp_seconds` | Epoch time of the last completed cycle (staleness alert) |

## Per table

| Metric | Meaning |
|--------|---------|
| `tierdb_cutline_tier_key{table}` | The cut-line `T` |
| `tierdb_cutline_snapshot{table}` | The pinned lake snapshot `S` |
| `tierdb_delta_backlog_rows{table}` | Correction rows awaiting compaction (growth = fold blocked or under-provisioned) |
| `tierdb_lake_commits_total{table}` | Lake snapshots committed |
| `tierdb_mirror_lag_bytes{table}` | Mirrored: WAL bytes between the server position and the mirror frontier |
| `tierdb_mirror_flushes_total{table}` | Mirrored: pump flushes committed |
| `tierdb_load_total{table,state}` | Stream loads by outcome (`committed`, `staged`, `replay`, `rejected`, `conflict`) |
| `tierdb_load_rows_total{table,path}` | Stream-loaded rows by path (`heap`, `delta`, `spool`) |
| `tierdb_load_staged_labels{table}` | Staged loads awaiting adoption into Iceberg |
| `tierdb_load_adoption_lag_seconds{table}` | Age of the oldest staged load (growth = adoption blocked) |

## Lake health

One gauge per counter the format plugin reports into `tierdb.lake_stats`, republished as `tierdb_lake_<counter>{table}`. The counter names are format-owned. Iceberg reports `files`, `delete_files`, `bytes`, `records`, `snapshots`, `manifests`, and `delete_ratio`. `tierdb_lake_warnings{table}` is the number of active health warnings, alert on it being nonzero.

## Per replication slot

| Metric | Meaning |
|--------|---------|
| `tierdb_slot_active{slot}` | Whether a consumer currently streams the slot |
| `tierdb_slot_retained_wal_bytes{slot}` | WAL pinned by the slot. Alert on growth, see the [WAL guard](../operations/day-2.md#slot-wal-retention-guard) |

## Housekeeping

| Metric | Meaning |
|--------|---------|
| `tierdb_expired_pins_deleted_total` | Expired read pins removed by the sweep |

## Suggested alerts

- `sum(tierdb_leader) != 1`: no leader, or two.
- `time() - tierdb_last_cycle_timestamp_seconds > 120`: the daemon stalled.
- `tierdb_slot_retained_wal_bytes` above your WAL budget: see the runbook.
- `tierdb_delta_backlog_rows` growing monotonically: compaction blocked.
- `tierdb_mirror_lag_bytes` sustained high: the pump can't keep up.
