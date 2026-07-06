# Operations

Day-2 concerns: auditing, offboarding, high availability, backpressure, WAL retention, and lake maintenance.

## Verify

```bash
tierdb-worker verify --table public.my_dim
```

Compares heap vs lake for mirrored tables (row count, tier-key min/max, and a PK checksum, exact on an idle table with the current replication drift reported on a live one) and audits journaled row counts per tiered partition. Exits non-zero on mismatch. Run it after onboarding and periodically from cron.

## Unregister

```bash
tierdb-worker unregister --table public.my_dim              # keep the lake table
tierdb-worker unregister --table public.my_dim --drop-lake  # purge it
```

Removes the catalog rows (cutline, partitions, delta, pins, and journal all cascade), drops the replication slot and publication, and resets `REPLICA IDENTITY`. The lake table survives by default, because for tiered tables it holds the only copy of reclaimed rows. `--drop-lake` purges it. Unregistering an unknown table is a no-op that still cleans up any leftover slot or publication.

## High availability

Run as many workers as you like against the same database. They campaign for a leader lease (a session-scoped Postgres advisory lock). Exactly one is active. The rest stand by and take over within `TIERDB_CAMPAIGN_INTERVAL_SECONDS` when the leader's lock session dies, including the mirrored tables' replication slots, whose stale holders are evicted automatically. Every advance is guarded by protocol (monotonic catalog updates, single-consumer slots, the pre-commit gap probe), so a delayed step-down cannot corrupt state.

## Backpressure

Mirror pump memory: a single transaction touching more than `TIERDB_MIRROR_MAX_BUFFERED_ROWS` rows (default 100k) is folded into the lake in intermediate commits that never advance the frontier or the slot. Readers never see a partial transaction, and worker memory stays flat even for a 100M-row `UPDATE`.

Delta backlog: the sweep logs WARN when a table's correction backlog passes `TIERDB_DELTA_BACKLOG_WARN_ROWS` (default 100k), and ERROR with a runbook at four times that. A growing backlog means compaction is blocked (usually a long-held read pin) or under-provisioned.

Read pins: pins carry `expires_at`. Expired pins are ignored by the reclaim and compaction horizon and deleted by the sweep, so a crashed reader can only stall reclaim until its pin expires.

## Slot WAL retention guard

A logical slot pins WAL until its consumer advances. If a mirror worker stays down, Postgres keeps WAL segments and the disk fills. Each sweep the daemon measures every `tierdb_*` slot and exports `tierdb_slot_retained_wal_bytes{slot}`. Above `TIERDB_SLOT_WARN_BYTES` it logs WARN, above four times that an ERROR with this runbook. The guard never drops a slot itself. When the alert fires:

1. Mirror worker down? Start it (or a standby, takeover is automatic). The slot drains and WAL is recycled.
2. Table abandoned? `unregister` it.
3. As a hard cap, set `max_slot_wal_keep_size` so a runaway slot can never fill the disk. Past it the slot is invalidated and the table needs a fresh `register`.

## Lake maintenance

The cold tier needs upkeep (small-file compaction, delete-debt compaction, snapshot expiry, metadata hygiene) and TierDB owns it by default, with per-pass and per-table opt-outs for teams that already run their own. It is a topic of its own: see [Lake maintenance](lake-maintenance.md), including what must stay with TierDB (anything that deletes files, snapshot expiry above all) and what you can safely run externally.

## Monitoring

Scrape `/metrics` (either binary) with Prometheus. The key series and their meanings are listed in the [metrics reference](../reference/metrics.md). The useful alerts: `tierdb_leader` sum not equal to 1, `tierdb_slot_retained_wal_bytes` growth, `tierdb_delta_backlog_rows` growth, and `tierdb_mirror_lag_bytes` sustained high.
