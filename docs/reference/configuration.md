# Configuration

The worker and console are configured entirely through environment variables.
The same binary points at any deployment shape.

## Connection

| Env var | Default | Meaning |
|---------|---------|---------|
| `MODAK_PG_URL` | `jdbc:postgresql://localhost:5432/postgres` | Catalog + hot tier (JDBC URL, passed verbatim, TLS params go here) |
| `MODAK_PG_USER` / `MODAK_PG_PASSWORD` | `postgres` / empty | Credentials |
| `MODAK_WAREHOUSE` | `/tmp/modak-warehouse` | Warehouse root (`s3://...` or a filesystem path) |
| `MODAK_S3_ENDPOINT` | unset | S3 endpoint. Unset = local filesystem or real AWS |
| `MODAK_S3_ACCESS_KEY` / `MODAK_S3_SECRET_KEY` | unset | S3 credentials (unset on AWS = default chain) |
| `MODAK_S3_REGION` | empty | S3 region |
| `MODAK_S3_SSL` | `false` | TLS to the S3 endpoint |

## Worker behaviour

| Env var | Default | Meaning |
|---------|---------|---------|
| `MODAK_CYCLE_INTERVAL_SECONDS` | `10` | Scheduler interval |
| `MODAK_TIERING_LAG` | `0` | Keep partitions hot until `max(tier_key) - lag` passes them |
| `MODAK_RECLAIM_LAG` | same as tiering lag | Extra (ceiling-based) lag before a tiered partition is dropped |
| `MODAK_COMPACTION_BATCH` | `1000` | Max delta rows folded per cycle |
| `MODAK_MIRROR_BATCH` | `500` | Mirror pump: rows per Iceberg commit |
| `MODAK_MIRROR_FLUSH_MILLIS` | `2000` | Mirror pump: max time before a partial batch commits |
| `MODAK_MIRROR_MAX_BUFFERED_ROWS` | `100000` | Memory bound: larger transactions fold intermediately (invisible to readers) |
| `MODAK_DELTA_BACKLOG_WARN_ROWS` | `100000` | Per-table delta backlog WARN threshold (ERROR at 4x) |
| `MODAK_CAMPAIGN_INTERVAL_SECONDS` | `5` | Standby retry interval for the leader lease |
| `MODAK_SLOT_WARN_BYTES` | `1073741824` (1 GiB) | Retained-WAL WARN threshold for the slot guard |
| `MODAK_PREMAKE_PARTITIONS` | `2` | Empty partition widths kept ahead of each table's write frontier. `0` disables premake |

## Lake maintenance

| Env var | Default | Meaning |
|---------|---------|---------|
| `MODAK_MAINTENANCE_INTERVAL_SECONDS` | `3600` | How often each table gets a maintenance pass |
| `MODAK_REWRITE_TARGET_BYTES` | `134217728` (128 MiB) | Data files smaller than this are bin-pack candidates |
| `MODAK_REWRITE_MIN_INPUT_FILES` | `8` | Small files that must accumulate before a rewrite runs |
| `MODAK_SNAPSHOT_RETENTION_HOURS` | `24` | Snapshots older than this are expirable |
| `MODAK_SNAPSHOT_MIN_RETAINED` | `5` | Snapshots always kept, regardless of age |

## Endpoints

| Env var | Default | Meaning |
|---------|---------|---------|
| `MODAK_METRICS_PORT` | unset | Headless worker: Prometheus `/metrics` port. Unset = no endpoint |
| `MODAK_CONSOLE_PORT` | `9090` | Console binary: the web console port (includes `/metrics`) |
| `MODAK_CONSOLE_SQL` | `true` | SQL playground. `false` disables the query endpoint |

## Iceberg catalog

By default the lake is path-based: each table lives directly under
`MODAK_WAREHOUSE` with no catalog service. Set `MODAK_CATALOG_URI` and tables
are instead created and loaded through an Iceberg REST catalog as
`<namespace>.<schema>_<table>`. The read path is identical either way: every
commit publishes the table's `metadata_location` into `modak.tables`, and
DuckDB scans that file directly.

| Env var | Default | Meaning |
|---------|---------|---------|
| `MODAK_CATALOG_URI` | unset | Iceberg REST catalog endpoint. Unset = path-based |
| `MODAK_CATALOG_WAREHOUSE` | unset | Warehouse location the catalog assigns to new tables |
| `MODAK_CATALOG_TOKEN` | unset | Bearer token, if the catalog requires one |
| `MODAK_CATALOG_NAMESPACE` | `modak` | Namespace for tables created through the catalog |
| `MODAK_LAKE_FORMAT` | `iceberg` | Lake format plugin id |

## Lake properties passthrough

Everything else a format supports flows through `MODAK_LAKE_PROPS`:
semicolon-separated `key=value` pairs handed verbatim to the format plugin. The
Iceberg plugin interprets three prefixes:

- `iceberg.catalog.<key>`: an Iceberg catalog property (OAuth2, custom
  headers, `io-impl`, ...).
- `iceberg.table.<key>`: a table property stamped at creation (`write.*`,
  `commit.*`, `history.*` families).
- `hadoop.<key>`: passed to the Hadoop configuration backing file IO.

```bash
MODAK_LAKE_PROPS='iceberg.table.write.parquet.compression-codec=zstd;iceberg.catalog.oauth2-server-uri=https://idp/token'
```
