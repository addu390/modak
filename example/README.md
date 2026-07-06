# Example

```bash
make -C example up          # postgres, worker, RustFS
make -C example scenarios   # every scenario, ~3 minutes
```

## make targets

| Target | What it does |
|--------|---------------|
| `up` | Build and start the base stack. `EXAMPLE_EMBEDDED=1` for embedded mode. |
| `down` | Tear down and remove all data. |
| `patch` | Rebuild and restart changed services, no teardown. |
| `status` | Show which overlays are up. |
| `add-trino`, `add-catalog`, `add-spark` | Layer an overlay onto whatever is running. |
| `remove-trino`, `remove-catalog`, `remove-spark` | Drop an overlay back out. |
| `scenario-<name>` | Run one scenario, e.g. `scenario-core`. |
| `scenarios` | Run every scenario; `trino`/`spark` skip unless their overlay is up. |

## Scenarios

Self-contained: each resets its own tables, seeds them, asserts its own results.

| Scenario | What it shows |
|----------|---------------|
| `core.sh` | Tiering, corrections folded via compaction, a pinned two-tier read, mirrored CDC, a cross-mode join, stream loads. |
| `lifecycle.sh` | An initial copy killed mid-flight resumes from its journal, `verify`, clean `unregister`. |
| `timestamptz.sh` | A `timestamptz` tier key: daily tiering, native timestamp reads and corrections. |
| `trino.sh` | Trino reads the two-tier view through the modak connector. Needs `EXAMPLE_TRINO=1 make -C example add-trino`. |
| `spark.sh` | `ModakSpark.read` spans both tiers, no SQL layer. Needs `EXAMPLE_SPARK=1 make -C example add-spark`. |

## Datasets

`datasets/<table>/schema.sql` is DDL, `datasets/<table>/*.jsonl` are batches (seed, updates, corrections, stream loads). `ingest.sh` is the only place that turns a file into rows:

| Mode | What it does |
|------|---------------|
| `insert` | Bulk insert. |
| `update --pk col` | `UPDATE` per row, present columns are the `SET` list. |
| `delete --pk col` | `DELETE` per row. |
| `modak-upsert` | `modak_upsert()` per row. |
| `modak-delete --pk col --tier-key col [--tier-key-type type]` | `modak_delete()` per row. |
| `stream-load --label name [--token tok]` | POST the file as one labeled HTTP batch. |
| `sql` | Run the file directly, for generated data. |

Tables (vehicle-fleet theme, one axis each): `trip_events/` (bigint tier key), `gps_pings/` (timestamptz tier key), `vehicles/` (mirrored), `tire_pressure_logs/` (20k rows, kill-and-resume copy), `live.sql` (higher-volume version for `live.sh`).

## Compose layering

`compose/modak-standalone.yml` or `modak-embedded.yml` picks the topology, `rustfs.yml` always follows, `lakekeeper.yml`/`trino.yml`/`spark.yml` layer on top. `make add-<x>`/`remove-<x>` control it without touching the rest; `EXAMPLE_CATALOG=1`/`EXAMPLE_TRINO=1`/`EXAMPLE_SPARK=1` pick the same overlays for scenario scripts independent of `make`.

`compose/trino/` and `compose/spark/` are sandbox-only, not deployment guides. For real setups see [Trino](../docs/integrations/trino.md) and [Spark](../docs/integrations/spark.md).

## Keeping it running

```bash
./example/live.sh          # setup (idempotent) + stream until Ctrl-C
./example/live.sh reset    # offboard the live tables
```

Streams `trip_events_live`/`gps_pings_live` (two storage profiles) with periodic cold corrections, so [the console](http://localhost:9090) has something live to show.
