# Storage profiles

A storage profile is a named warehouse binding: a warehouse root, the lake format, non-secret config overrides, and a credential reference. Tables pick a profile at registration, so one deployment can spread tables across buckets, accounts, regions, or entirely different object stores.

Every deployment starts with a seeded `default` profile whose warehouse and format are blank, meaning "resolve from the worker's environment" (`TIERDB_WAREHOUSE`, `TIERDB_LAKE_FORMAT`, `TIERDB_LAKE_PROPS`). Single-warehouse deployments never need to touch profiles.

## Creating a profile

```bash
tierdb-worker profile create --name analytics \
    --warehouse s3://analytics-lake/warehouse \
    --config 'iceberg.table.write.parquet.compression-codec=zstd' \
    --credentials analytics

tierdb-worker profile list
```

| Flag | Meaning |
|------|---------|
| `--name` | Profile name, referenced at `register` time |
| `--warehouse` | Warehouse root (`s3://...`, `gs://...`, or a filesystem path) |
| `--format` | Lake format plugin id. Omit to use the worker's `TIERDB_LAKE_FORMAT` |
| `--config` | Semicolon-separated `key=value` overrides, same keys as `TIERDB_LAKE_PROPS`. A blank value (`key=`) removes an inherited default |
| `--credentials` | Credential reference (see below). Omit to use the worker's default credentials |
| `--default` | Make this the default profile for new registrations |

The console can also list and create profiles (`GET`/`POST /api/v1/storage-profiles`).

## Using a profile

```bash
tierdb-worker register --table public.events --pk id --tier-key ts --profile analytics
```

The table's lake lives under the profile's warehouse from then on. The profile is recorded in `tierdb.tables.storage_profile` and every lake-touching path (tiering, compaction, ingest, stream load, maintenance, verify) resolves storage through it. Direct tables attach the lake catalog live, so their profile must also carry a catalog endpoint (`--config catalog.uri=...`, or `TIERDB_CATALOG_URI` on the default profile); registration fails fast without one.

## Config resolution

A table's effective lake config is layered, later layers win:

1. The worker's environment (`TIERDB_WAREHOUSE`, `TIERDB_S3_*`, `TIERDB_CATALOG_*`, `TIERDB_LAKE_PROPS`).
2. The profile's warehouse and `--config` overrides.
3. The credential fragment named by `--credentials`.

Config is an opaque `key=value` map interpreted by the format plugin, not by TierDB, so a profile can target any store the plugin's IO layer supports: S3, GCS, Azure, HDFS, or a local path.

## Credentials

Secrets never enter the catalog. A profile stores only a *reference*: `--credentials analytics` means the worker resolves the environment variable `TIERDB_CREDENTIALS_ANALYTICS` at use time, semicolon-separated `key=value` pairs merged over the config:

```bash
# S3-compatible
TIERDB_CREDENTIALS_ANALYTICS='s3.access-key=AKIA...;s3.secret-key=...;s3.region=us-east-2'

# GCS via the Hadoop connector
TIERDB_CREDENTIALS_GCSEU='hadoop.fs.gs.auth.service.account.json.keyfile=/secrets/gcs.json'
```

A worker that lacks the referenced variable fails loudly the first time it touches a table on that profile. A blank value removes an inherited key, e.g. `s3.access-key=` drops the default credentials and falls back to the provider's ambient chain (instance roles, workload identity).

## The read path

Workers write; `iceberg_scan()` inside Postgres reads, and DuckDB needs its own secret per warehouse. Register one scoped secret per profile:

```sql
SELECT duckdb.create_simple_secret(
    type   := 'S3',
    key_id := '...',
    secret := '...',
    scope  := 's3://analytics-lake/'
);
```

The compose stack automates this: any `TIERDB_READ_SECRET_<NAME>` environment variable on the `postgres` service is parsed as `key=value` pairs and passed through to `duckdb.create_simple_secret()` at init, so any provider DuckDB supports works the same way.
