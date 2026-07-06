# Quickstart

Run the full TierDB loop locally in about ten minutes: a Postgres with the extension, RustFS standing in for S3, the worker, and a scripted walkthrough.

## Prerequisites

- Docker with the compose plugin.
- About 4 GB free for the images. They are pulled prebuilt from GHCR; compose only compiles from source if the pull fails.

## Start the stack

```bash
git clone --recurse-submodules https://github.com/Modak-Labs/tierdb && cd tierdb
make -C example up
```

`example/compose/tierdb-standalone.yml` wires together the two images TierDB ships, Postgres with the extension and the worker, parameterized by env vars for your own Postgres and S3 (see [Production deployment](../operations/production.md)). `example/compose/rustfs.yml` layers in RustFS, a local S3-compatible stand-in, so the stack above is runnable without a cloud account:

| Service | Role |
|---------|------|
| `postgres` | Postgres 17 + `pg_duckdb` + the `tierdb` extension + `tierdb.*` catalog |
| `rustfs` | S3-compatible Iceberg warehouse (`s3://warehouse`) |
| `worker` | The daemon (console binary): tiering, mirroring, compaction |

## Run the example

```bash
make -C example scenarios
```

Asserts tiering, corrections, CDC, and lifecycle end to end. Each scenario is a separate script under `example/scenarios/`, runnable alone with `make -C example scenario-core`. See [`example/README.md`](https://github.com/Modak-Labs/tierdb/blob/main/example/README.md) for the full list.

## Poke around

```bash
psql postgres://postgres:tierdb@localhost:5432/postgres   # the database
open http://localhost:9090                               # the TierDB console
open http://localhost:9001                               # RustFS console (rustfs-root-user/rustfs-root-password)
(cd example && docker compose logs -f worker)             # cycle-by-cycle log
```

Try a transparent read on the tiered table the example created:

```sql
SELECT * FROM public.trip_events ORDER BY id;       -- spans both tiers, one table
SET tierdb.transparent_reads = off;
SELECT * FROM public.trip_events ORDER BY id;       -- raw heap: only the hot slice
```

## Register your own table

Tiered mode needs `PARTITION BY RANGE` on the tier key, a timestamp, date, or integer column. Mirrored mode takes any table with a primary key:

```bash
cd example
docker compose run --rm worker register \
    --table public.my_table --pk id --tier-key event_time                 # tiered
docker compose run --rm worker register \
    --table public.my_dim --pk id --tier-key updated_at --mode mirrored   # mirrored
```

See [Registering tables](../tables/registering-tables.md) for modes, retention, and composite keys.

## Teardown

```bash
make -C example down    # removes all data
```

!!! note
    The compose stack is a test harness, not a production topology. The same worker binary points at your managed Postgres and real object store through the same env vars. See [Production deployment](../operations/production.md).
