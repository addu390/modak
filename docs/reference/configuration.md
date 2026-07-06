# Configuration

The worker and console are configured entirely through environment variables. The same binary points at any deployment shape.

## Connection

| Env var | Default | Meaning |
|---------|---------|---------|
| `TIERDB_PG_URL` | `jdbc:postgresql://localhost:5432/postgres` | Catalog + hot tier (JDBC URL, passed verbatim, TLS params go here) |
| `TIERDB_PG_USER` / `TIERDB_PG_PASSWORD` | `postgres` / empty | Credentials |
| `TIERDB_WAREHOUSE` | `/tmp/tierdb-warehouse` | Warehouse root (`s3://...` or a filesystem path) |
| `TIERDB_S3_ENDPOINT` | unset | S3 endpoint. Unset = local filesystem or real AWS |
| `TIERDB_S3_ACCESS_KEY` / `TIERDB_S3_SECRET_KEY` | unset | S3 credentials (unset on AWS = default chain) |
| `TIERDB_S3_REGION` | empty | S3 region |
| `TIERDB_S3_SSL` | `false` | TLS to the S3 endpoint |
| `TIERDB_CREDENTIALS_<REF>` | unset | Named credential set for storage profiles: `key=value` pairs, `;`-separated, merged into the lake config of any profile created with `--credentials <ref>`. See [Storage profiles](../tables/storage-profiles.md) |

The `TIERDB_WAREHOUSE`/`TIERDB_S3_*` settings define the default warehouse. Deployments with more than one warehouse layer [storage profiles](../tables/storage-profiles.md) on top.

## Worker behaviour

| Env var | Default | Meaning |
|---------|---------|---------|
| `TIERDB_CYCLE_INTERVAL_SECONDS` | `10` | Scheduler interval |
| `TIERDB_TIERING_LAG` | `0` | Keep partitions hot until `max(tier_key) - lag` passes them |
| `TIERDB_RECLAIM_LAG` | same as tiering lag | Extra (ceiling-based) lag before a tiered partition is dropped |
| `TIERDB_COMPACTION_BATCH` | `1000` | Max delta rows folded per cycle |
| `TIERDB_MIRROR_BATCH` | `500` | Mirror pump: rows per Iceberg commit |
| `TIERDB_MIRROR_FLUSH_MILLIS` | `2000` | Mirror pump: max time before a partial batch commits |
| `TIERDB_MIRROR_MAX_BUFFERED_ROWS` | `100000` | Memory bound: larger transactions fold intermediately (invisible to readers) |
| `TIERDB_DELTA_BACKLOG_WARN_ROWS` | `100000` | Per-table delta backlog WARN threshold (ERROR at 4x) |
| `TIERDB_CAMPAIGN_INTERVAL_SECONDS` | `5` | Standby retry interval for the leader lease |
| `TIERDB_SLOT_WARN_BYTES` | `1073741824` (1 GiB) | Retained-WAL WARN threshold for the slot guard |
| `TIERDB_PREMAKE_PARTITIONS` | `2` | Empty partition widths kept ahead of each table's write frontier. `0` disables premake |

## Lake maintenance

These are the worker-wide defaults. Any of the equivalent settings can be overridden per table with `tierdb-worker policy`, see [Lake maintenance](../operations/lake-maintenance.md).

| Env var | Default | Meaning |
|---------|---------|---------|
| `TIERDB_MAINTENANCE_ENABLED` | `true` | Fleet-wide default for the maintenance pass. Tables can re-enable or disable via policy |
| `TIERDB_MAINTENANCE_INTERVAL_SECONDS` | `3600` | How often each table gets a maintenance pass |
| `TIERDB_MAINTENANCE_ENGINE` | `embedded` | What executes maintenance plans. `embedded` runs in the worker, external engines are the extension point |
| `TIERDB_LAKE_STATS_INTERVAL_SECONDS` | `60` | How often each table's lake health snapshot is refreshed |
| `TIERDB_REWRITE_TARGET_BYTES` | `134217728` (128 MiB) | Data files smaller than this are bin-pack candidates |
| `TIERDB_REWRITE_MIN_INPUT_FILES` | `8` | Small files that must accumulate before a rewrite runs |
| `TIERDB_SNAPSHOT_RETENTION_HOURS` | `24` | Snapshots older than this are expirable |
| `TIERDB_SNAPSHOT_MIN_RETAINED` | `5` | Snapshots always kept, regardless of age |

## Endpoints

| Env var | Default | Meaning |
|---------|---------|---------|
| `TIERDB_METRICS_PORT` | unset | Headless worker: Prometheus `/metrics` port. Unset = no endpoint |
| `TIERDB_CONSOLE_PORT` | `9090` | Console binary: the web console port (includes `/metrics`) |
| `TIERDB_CONSOLE_SQL` | `true` | SQL playground. `false` disables the query endpoint |
| `TIERDB_LOAD_TOKEN` | unset | Enables `POST /api/load` (see [Stream load](../ingestion/stream-load.md)). Unset = no endpoint |
| `TIERDB_LOAD_SPOOL_THRESHOLD` | `1000` | Cold rows per batch above which a load spools Parquet instead of the delta |

## Iceberg catalog

By default the lake is path-based: each table lives directly under `TIERDB_WAREHOUSE` with no catalog service. Set `TIERDB_CATALOG_URI` and tables are instead created and loaded through an Iceberg REST catalog as `<namespace>.<schema>_<table>`. The read path is identical either way: every commit publishes the table's `metadata_location` into `tierdb.tables`, and DuckDB scans that file directly.

| Env var | Default | Meaning |
|---------|---------|---------|
| `TIERDB_CATALOG_URI` | unset | Iceberg REST catalog endpoint. Unset = path-based |
| `TIERDB_CATALOG_WAREHOUSE` | unset | Warehouse location the catalog assigns to new tables |
| `TIERDB_CATALOG_TOKEN` | unset | Bearer token, if the catalog requires one |
| `TIERDB_CATALOG_NAMESPACE` | `tierdb` | Namespace for tables created through the catalog |
| `TIERDB_LAKE_FORMAT` | `iceberg` | Lake format plugin id |

## Lake properties passthrough

Everything else a format supports flows through `TIERDB_LAKE_PROPS`: semicolon-separated `key=value` pairs handed verbatim to the format plugin. The Iceberg plugin interprets three prefixes:

- `iceberg.catalog.<key>`: an Iceberg catalog property (OAuth2, custom headers, `io-impl`, ...).
- `iceberg.table.<key>`: a table property stamped at creation (`write.*`, `commit.*`, `history.*` families).
- `hadoop.<key>`: passed to the Hadoop configuration backing file IO.

```bash
TIERDB_LAKE_PROPS='iceberg.table.write.parquet.compression-codec=zstd;iceberg.catalog.oauth2-server-uri=https://idp/token'
```
