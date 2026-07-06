# Lake maintenance

A lakehouse degrades without upkeep. Streaming-sized commits pile up small files, merge-on-read folds accumulate delete files that slow every scan, snapshots and manifests grow without bound, and a crash can strand files no commit ever adopted. TierDB owns this lifecycle by default: every table gets a maintenance pass on a schedule, driven by a per-table policy, inside safety bounds only TierDB can compute.

Maintenance is also optional. Every pass can be disabled per table or fleet wide, so a team that already runs its own compaction keeps doing exactly that. What is not optional is the safety story below.

## Who owns what

The design splits ownership along the same seam as the rest of TierDB:

- **TierDB owns scheduling, policy, and safety.** It decides when a pass runs, resolves the settings, and computes two bounds every pass must honor: no snapshot at or above the oldest pinned reader horizon may expire (pinned reads scan an old `metadata_location` whose files must stay put), and staged [Stream Load](../ingestion/stream-load.md) files awaiting adoption must never be deleted.
- **The format plugin owns the work.** Which operations exist, what the policy keys mean, and what a pass reports are Iceberg's business today and a future format's tomorrow. Nothing in the catalog, console, or metrics assumes Iceberg's vocabulary.

### What must stay with TierDB

The dividing line is whether an operation deletes files:

| Operation | Externally safe? | Why |
|-----------|------------------|-----|
| Small-file bin-pack | Yes | Snapshot-additive, old snapshots stay readable |
| Delete-debt compaction | Yes | Snapshot-additive |
| Manifest rewrite | Yes | Snapshot-additive, metadata only |
| Snapshot expiry | **No** | Deletes data files. Only TierDB knows the pinned reader horizon |
| Orphan file removal | **No** | Deletes files by listing. Only TierDB knows which staged loads await adoption |

!!! danger "Never run an external `expire_snapshots` or `remove_orphan_files`"
    Spark's and Trino's maintenance procedures do not know about TierDB's read pins or staged loads. An external expiry can delete data files a pinned reader is scanning mid-query, and an external orphan sweep can delete staged files a load is about to adopt. Keep both with TierDB, whatever else you disable.

### Bringing your own maintenance

If you already run compaction through Spark, Amoro, or a catalog service, disable the rewrite passes and let TierDB keep only the file-deleting ones:

```bash
tierdb-worker policy --table public.events \
  --set rewrite_enabled=false \
  --set delete_compaction_enabled=false \
  --set manifest_rewrite_enabled=false
```

Snapshot expiry stays on and pin-safe. Disabling it too (`snapshot_expiry_enabled=false`) is allowed but means nothing expires snapshots for that table, and nothing else safely can, so plan for metadata and storage growth.

The longer-term answer is the engine seam: maintenance runs as a `MaintenancePlan` handed to a `MaintenanceEngine`, and the plan carries the safety bounds. `TIERDB_MAINTENANCE_ENGINE=embedded` (the in-worker engine) is the only engine so far. An external engine such as Spark would receive the same plan and the same bounds, which is the supported way to move the heavy lifting out of the worker without giving up safety.

## Enabling and disabling

Everything layers: worker env defaults first, then the table's policy.

| Setting | Default | Scope |
|---------|---------|-------|
| `TIERDB_MAINTENANCE_ENABLED` | `true` | Fleet-wide default for the whole pass |
| `maintenance_enabled` | env | Per table, the whole pass |
| `rewrite_enabled` | `true` | Per table or env-defaulted, small-file bin-pack |
| `delete_compaction_enabled` | `true` | Delete-debt compaction |
| `manifest_rewrite_enabled` | `true` | Manifest rewrite |
| `snapshot_expiry_enabled` | `true` | Snapshot expiry |
| `orphan_sweep_enabled` | `false` | Listing-based orphan sweep, opt-in |

Disabling maintenance does not disable monitoring or TierDB's own bookkeeping: lake health collection keeps running, and staged files of *failed* loads are still cleaned up from the load journal (that is journal-driven, not listing-driven, and touches only files TierDB itself staged).

## Policy

Per-table settings live in the catalog (`tierdb.tables.maintenance_policy`) and are edited with the [`policy` command](../reference/cli.md#policy):

```bash
tierdb-worker policy --table public.events                                # view
tierdb-worker policy --table public.events --set snapshot_retention_hours=6
tierdb-worker policy --table public.events --unset snapshot_retention_hours
tierdb-worker policy --table public.events --reset                        # back to defaults
```

The view marks each setting `(table)` or `(default)` so the resolution is never a guess. The full key set the Iceberg plugin understands:

| Key | Default | Meaning |
|-----|---------|---------|
| `maintenance_enabled` | env | Master switch for the table's maintenance pass |
| `rewrite_enabled` | `true` | Small-file bin-pack on/off |
| `rewrite_target_bytes` | env | Data files smaller than this are bin-pack candidates |
| `rewrite_min_input_files` | env | Small files that must accumulate before a rewrite runs |
| `delete_compaction_enabled` | `true` | Delete-debt compaction on/off |
| `delete_compaction_min_deletes` | `1` | Delete files a data file must carry before it is rewritten |
| `manifest_rewrite_enabled` | `true` | Manifest rewrite on/off |
| `manifest_rewrite_min_manifests` | `100` | Manifest count that triggers a manifest rewrite |
| `snapshot_expiry_enabled` | `true` | Snapshot expiry on/off |
| `snapshot_retention_hours` | env | Snapshots older than this are expirable |
| `snapshot_min_retained` | env | Snapshots always kept, regardless of age |
| `orphan_sweep_enabled` | `false` | Opt-in listing-based orphan file sweep |
| `orphan_grace_hours` | `72` | Age before an unreferenced or failed-load file may be deleted |

Env defaults for the `env` rows are in the [configuration reference](../reference/configuration.md#lake-maintenance).

## What a pass does (Iceberg)

Every table gets a pass every `TIERDB_MAINTENANCE_INTERVAL_SECONDS` (default hourly). Mirrored tables need it most, since the pump commits one snapshot per flush.

Delete-debt compaction: a data file carrying at least `delete_compaction_min_deletes` delete files is rewritten with the deletes applied, and delete files no surviving data file needs are dropped. Merge-mode folds write equality deletes, so mirrored and keep-heap tables build this debt in normal operation.

Small-file bin-pack: once `rewrite_min_input_files` data files smaller than `rewrite_target_bytes` accumulate, they are rewritten into one file per partition in a single atomic commit. Files still under deletes are left to the compaction pass above.

Manifest rewrite: past `manifest_rewrite_min_manifests` manifests, the manifest list is folded and clustered by partition. Metadata files themselves are bounded from birth, tables are created with `write.metadata.delete-after-commit.enabled` so old metadata JSON is pruned on commit.

Snapshot expiry: snapshots older than `snapshot_retention_hours` are expired, always keeping `snapshot_min_retained` and never crossing the pinned horizon.

Orphan sweep (opt-in): files in the table's data directory older than `orphan_grace_hours` that no retained snapshot references are deleted. Only a crash between write and commit produces such files, so it is off by default. Separately and always on, staged files of loads that ended in `failed` are deleted after the same grace period, driven by the load journal rather than listing.

Every non-noop pass is journaled in `tierdb.op_log` with its counters, and the console's table page shows the last pass next to the policy in force.

## Forcing a pass

A pass can be requested out of schedule, from the CLI or the console's table page:

```bash
tierdb-worker maintain --table public.events            # wait and print the result
tierdb-worker maintain --table public.events --no-wait  # file and return
```

The request is a row in `tierdb.maintenance_requests` that the leader claims atomically on its next cycle, so a forced pass runs under the same coordination as a scheduled one and never concurrently with it. One request per table is pending at a time, repeating the command just refreshes it.

## Lake health

Every `TIERDB_LAKE_STATS_INTERVAL_SECONDS` the worker refreshes `tierdb.lake_stats`: the plugin's counters, its health warnings (Iceberg warns on delete-file debt and manifest sprawl), and the policy in force. The [console](console.md) shows it per table, `/metrics` exports it as `tierdb_lake_*{table}` gauges (see the [metrics reference](../reference/metrics.md#lake-health)), and new warnings are logged once as WARN.

Health collection runs regardless of whether maintenance is enabled, so a table maintained externally still shows its file counts, delete debt, and warnings in the console. If you disabled a pass and its warning keeps firing, that is the signal your external maintenance is not keeping up.
