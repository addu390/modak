# Build recipes

`Dockerfile.postgres` and `Dockerfile.worker` are how the two published images get built, Postgres 17 with `pg_duckdb` and the `modak` extension baked in, and the worker daemon (tiering, mirroring, compaction). Nothing here is customer-facing. A deployment pulls the published images and configures them through env vars, see the [production deployment guide](../docs/operations/production.md), it has the full list and what to change.

`initdb/` provisions the Postgres image on first boot: extensions, then catalog schema, then a DuckDB S3 secret. `--build-arg MODAK_BINARY=modak-worker` on `Dockerfile.worker` builds the headless binary instead of the default `modak-console` (worker plus web console).

For a local sandbox to try Modak end to end without any cloud account, see [`example/README.md`](../example/README.md).
