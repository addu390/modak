# Example

A scripted walkthrough of the full Modak loop against the local docker stack.

```bash
docker compose up -d --build   # from the repo root, once
./example/run.sh               # the whole walkthrough, ~3 minutes
```

Every step asserts its results and fails fast, so this doubles as a smoke
test. Steps are re-runnable and can be run individually, though later steps
assume the earlier ones ran.

| Step | What it shows |
|------|---------------|
| `steps/00-reset.sh` | Offboards any previous run (`unregister --drop-lake`) so the example is re-runnable. |
| `steps/01-tiering.sh` | A tiered table: old partitions move to Iceberg, dropped from Postgres, and a plain `SELECT` still sees every row. |
| `steps/02-corrections.sh` | Corrections to cold rows via `modak_upsert`/`modak_delete`, folded into Iceberg by compaction, then one pinned read spanning both tiers. |
| `steps/03-mirroring.sh` | A mirrored table: plain DML trails into an Iceberg mirror via CDC, the same rows served from the heap and from the lake, and a cross-mode join. |
| `steps/04-lifecycle.sh` | Operations: an initial copy killed mid-flight resumes from its journal, `verify` proves heap and lake match, and `unregister` leaves nothing behind. |
| `steps/05-stream-load.sh` | Stream Load: labeled micro-batches over HTTP routed per row (heap and delta), and a replayed label returning its recorded result without applying anything. |

The tables live in `datasets/`:

- `events.sql`: a range-partitioned event stream (the tiered table).
- `vehicles.sql`: an ordinary OLTP table (the mirrored table).
- `telemetry.sql`: 20k rows for the kill-and-resume copy.

To run against the Iceberg REST catalog stack instead of the default
path-based warehouse:

```bash
EXAMPLE_REST=1 ./example/run.sh
```
