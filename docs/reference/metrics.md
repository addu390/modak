# Metrics

Prometheus text format on `/metrics`, served by the headless worker (set
`MODAK_METRICS_PORT`) and by the console binary (on `MODAK_CONSOLE_PORT`)
identically.

## Daemon

| Metric | Meaning |
|--------|---------|
| `modak_leader` | 1 on the leading worker, 0 on standbys (fleet sum should be exactly 1) |
| `modak_cycle_duration_seconds` | Duration of the last work cycle |
| `modak_last_cycle_timestamp_seconds` | Epoch time of the last completed cycle (staleness alert) |

## Per table

| Metric | Meaning |
|--------|---------|
| `modak_cutline_tier_key{table}` | The cut-line `T` |
| `modak_cutline_snapshot{table}` | The pinned lake snapshot `S` |
| `modak_delta_backlog_rows{table}` | Correction rows awaiting compaction (growth = fold blocked or under-provisioned) |
| `modak_lake_commits_total{table}` | Lake snapshots committed |
| `modak_mirror_lag_bytes{table}` | Mirrored: WAL bytes between the server position and the mirror frontier |
| `modak_mirror_flushes_total{table}` | Mirrored: pump flushes committed |
| `modak_load_total{table,state}` | Stream loads by outcome (`committed`, `staged`, `replay`, `rejected`, `conflict`) |
| `modak_load_rows_total{table,path}` | Stream-loaded rows by path (`heap`, `delta`, `spool`) |
| `modak_load_staged_labels{table}` | Staged loads awaiting adoption into Iceberg |
| `modak_load_adoption_lag_seconds{table}` | Age of the oldest staged load (growth = adoption blocked) |

## Per replication slot

| Metric | Meaning |
|--------|---------|
| `modak_slot_active{slot}` | Whether a consumer currently streams the slot |
| `modak_slot_retained_wal_bytes{slot}` | WAL pinned by the slot. Alert on growth, see the [WAL guard](../guides/operations.md#slot-wal-retention-guard) |

## Housekeeping

| Metric | Meaning |
|--------|---------|
| `modak_expired_pins_deleted_total` | Expired read pins removed by the sweep |

## Suggested alerts

- `sum(modak_leader) != 1`: no leader, or two.
- `time() - modak_last_cycle_timestamp_seconds > 120`: the daemon stalled.
- `modak_slot_retained_wal_bytes` above your WAL budget: see the runbook.
- `modak_delta_backlog_rows` growing monotonically: compaction blocked.
- `modak_mirror_lag_bytes` sustained high: the pump can't keep up.
