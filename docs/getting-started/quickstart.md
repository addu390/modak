# Quickstart

Run the full Modak loop locally in about ten minutes: a Postgres with the
extension, MinIO standing in for S3, the worker, and a scripted walkthrough.

## Prerequisites

- Docker with the compose plugin.
- About 4 GB free for the images. The first build compiles the extension and
  takes a few minutes.

## Start the stack

```bash
git clone --recurse-submodules https://github.com/Modak-Labs/modak && cd modak
docker compose up -d --build
```

Three services come up:

| Service | Role |
|---------|------|
| `postgres` | Postgres 17 + `pg_duckdb` + the `modak` extension + `modak.*` catalog |
| `minio` | S3-compatible Iceberg warehouse (`s3://warehouse`) |
| `worker` | The daemon (console binary): tiering, mirroring, compaction |

## Run the example

```bash
./example/run.sh
```

The walkthrough registers three tables and asserts each step. A tiered events
table has its old partitions moved to Iceberg and dropped from Postgres while
a plain `SELECT` still sees every row. Corrections get folded by compaction.
A mirrored vehicles table takes plain DML that CDC trails into Iceberg and
reads back from the lake. The lifecycle step kills an initial copy mid-flight
and watches it resume from its journal, runs `verify` to prove heap and lake
match, and finishes with `unregister` leaving nothing behind. Each step is a
separate script under `example/steps/` if you want to follow along one concept
at a time.

## Poke around

```bash
psql postgres://postgres:modak@localhost:5432/postgres   # the database
open http://localhost:9090                               # the Modak console
open http://localhost:9001                               # MinIO (minioadmin/minioadmin)
docker compose logs -f worker                            # cycle-by-cycle log
```

Try a transparent read on the tiered table the example created:

```sql
SELECT * FROM public.events ORDER BY id;       -- spans both tiers, one table
SET modak.transparent_reads = off;
SELECT * FROM public.events ORDER BY id;       -- raw heap: only the hot slice
```

## Register your own table

Tiered mode needs `PARTITION BY RANGE` on a bigint tier key. Mirrored mode
takes any table with a primary key:

```bash
docker compose run --rm worker register \
    --table public.my_table --pk id --tier-key event_time                 # tiered
docker compose run --rm worker register \
    --table public.my_dim --pk id --tier-key updated_at --mode mirrored   # mirrored
```

See [Registering tables](../tables/registering-tables.md) for modes, retention,
and composite keys.

## Teardown

```bash
docker compose down -v    # removes all data
```

!!! note
    The compose stack is a test harness, not a production topology. The same
    worker binary points at your managed Postgres and real object store through
    the same env vars. See [Production deployment](../operations/production.md).
