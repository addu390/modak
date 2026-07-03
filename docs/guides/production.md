# Production deployment

!!! warning "Modak is beta software"
    There is no stable release yet. The catalog schema, the SQL surface, and
    the CLI can change between versions, and some operational gaps are still
    open (see [Known gaps](#known-gaps-planned) below). Run it where you can
    tolerate that, and treat the lake plus your Postgres backups as the
    recovery story.

The compose stack is a test harness. In production the same worker binary runs
wherever you run services (VM, k8s, ECS, ...) and points at your Postgres and
your object store purely through env vars. This page covers what changes when
the database is shared, managed, and long-lived: roles, encryption, WAL
safety, and upgrades.

## What is Modak, and what is yours

You bring these, and Modak never owns or hosts them:

- Postgres. Your cluster, whether self-hosted or on a k8s operator. The one
  constraint: transparent reads need the `modak` and `pg_duckdb` extensions
  loaded, which managed services (vanilla RDS or Cloud SQL) forbid. The
  worker still runs against them, with reads through
  [connectors](../connectors/index.md). See
  [Deploying on AWS](aws.md). Mirrored tables additionally need
  `wal_level = logical`.
- Object store. Any S3-compatible endpoint (AWS S3, MinIO, Ceph, GCS via
  interop) holding the Iceberg warehouse. Modak writes standard Iceberg, and
  Spark, Trino, or DuckDB read it with no Modak anywhere.
- Iceberg catalog, optional. Any Iceberg REST catalog (Lakekeeper, Polaris,
  Nessie, Tabular). Unset, tables are path-based under the warehouse with zero
  extra services. Glue is not REST, so front it with a REST bridge or run
  without a catalog.
- Observability stack. Prometheus, Grafana, and alerting scrape the worker's
  `/metrics`. Modak does not ship or require them.

Modak core, what you actually deploy:

- The `modak` Postgres extension, installed into your Postgres. The write
  routers, planner hook, and read-pin logic.
- The `modak.*` catalog schema, plain tables in your database, created and
  migrated automatically by the worker at startup.
- The worker daemon (`modak-worker.jar`), the only long-running process Modak
  adds. Run at least two. They campaign for a leader lease and fail over
  automatically.

Optionally, the console (`modak-console.jar`), a strict superset of the worker
binary that additionally serves the web console. Run it instead of the worker
where operators want the UI, or skip it entirely.

A minimal production deployment:

```bash
MODAK_PG_URL='jdbc:postgresql://db.internal:5432/app?sslmode=verify-full'
MODAK_PG_USER=modak_worker
MODAK_WAREHOUSE=s3://analytics-lake/modak
MODAK_S3_REGION=us-east-1              # credentials via the AWS default chain
MODAK_CATALOG_URI=https://catalog.internal/iceberg   # optional REST catalog
MODAK_METRICS_PORT=9090                # headless: /metrics only
java -jar modak-worker.jar run
```

## Topology

- Postgres: any Postgres 16+ with `wal_level = logical`, the `modak` and
  `pg_duckdb` extensions installed, and
  `shared_preload_libraries = 'pg_duckdb, modak'` for transparent reads.
  [Installation](../getting-started/installation.md) walks through the full
  postgresql.conf and DuckDB setup.
- Workers: one or more instances. They campaign for a leader lease (a
  session-scoped advisory lock) and fail over automatically, including the
  mirrored tables' replication slots, whose stale holders are evicted.
- Object store: S3, GCS, MinIO, or a filesystem holding the warehouse,
  optionally fronted by an Iceberg REST catalog (`MODAK_CATALOG_URI`).

## Roles and grants

Two roles cover everything, and neither needs superuser.

`modak_admin` runs `register`, `unregister`, and `verify`. Registration alters
the table (`REPLICA IDENTITY`), creates a publication, and creates a
replication slot:

```sql
CREATE ROLE modak_admin LOGIN REPLICATION PASSWORD '...';
GRANT CREATE ON DATABASE app TO modak_admin;        -- CREATE PUBLICATION
GRANT app_owner TO modak_admin;                     -- own the tables it onboards
GRANT USAGE, CREATE ON SCHEMA modak TO modak_admin; -- catalog rows + migrations
```

`REPLICA IDENTITY`, `CREATE PUBLICATION ... FOR TABLE`, and partition `DROP`
all require table ownership. Membership in the owning role is the clean way
to get it.

`modak_worker` runs the daemon. It streams the slots, reads user tables for
tiering, drops tiered partitions, and reads and writes the `modak` schema:

```sql
CREATE ROLE modak_worker LOGIN REPLICATION PASSWORD '...';
GRANT USAGE ON SCHEMA modak TO modak_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA modak TO modak_worker;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO modak_worker;  -- tiering reads
GRANT app_owner TO modak_worker;                    -- partition DROP on reclaim
```

The daemon applies pending catalog migrations at startup. To restrict
migrations to `modak_admin`, revoke `CREATE` on the `modak` schema from the
worker and run each binary upgrade's first start as the admin.

Readers need nothing special, since transparent reads run as the querying
user:

```sql
GRANT USAGE ON SCHEMA modak TO app_readers;
GRANT SELECT ON ALL TABLES IN SCHEMA modak TO app_readers;
GRANT INSERT, DELETE ON modak.read_pins TO app_readers;
```

## TLS

Postgres: `MODAK_PG_URL` is passed to JDBC verbatim. Use
`sslmode=verify-full` (not `require`) so the server identity is actually
checked. The registrar's replication connection uses the same URL.

```
MODAK_PG_URL='jdbc:postgresql://db.internal:5432/app?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem'
```

Object store and catalog: `MODAK_S3_SSL=true` turns on TLS to the S3 endpoint.
Anything more specific (custom CA bundles, KMS, REST catalog OAuth) flows
through the `MODAK_LAKE_PROPS` passthrough. See
[Configuration](../reference/configuration.md#lake-properties-passthrough).

HTTP endpoints: the metrics endpoint and the console are plain HTTP with no
auth by design. Bind them to an internal interface, or front them with an
authenticating reverse proxy. The console's SQL playground executes arbitrary
statements with the worker's credentials, so set `MODAK_CONSOLE_SQL=false`
unless the port is genuinely restricted to trusted operators, or run the
headless worker binary, which has no console at all.

## WAL safety

Every mirrored table holds a logical replication slot, and a slot pins WAL
until its consumer advances. Two layers of protection:

1. The worker's slot guard exports `modak_slot_retained_wal_bytes{slot}` and
   logs WARN and ERROR past `MODAK_SLOT_WARN_BYTES` (default 1 GiB).
2. Set the hard cap in Postgres so a dead consumer can never fill the disk:

    ```sql
    ALTER SYSTEM SET max_slot_wal_keep_size = '20GB';
    SELECT pg_reload_conf();
    ```

Size it to survive your longest tolerable worker outage at peak WAL rate.
Past the cap Postgres invalidates the slot, and the table then needs
`unregister` plus `register` (fresh initial copy). That is a deliberate trade
against an unbounded disk.

`max_wal_senders` and `max_replication_slots` must exceed the number of
mirrored tables. Each holds one slot, and only the leader streams them.

## Catalog schema upgrades

`modak.schema_meta` records the installed catalog version. On startup the
worker applies any pending migration under an advisory lock, and refuses to
start if the database is newer than the binary. During a rolling upgrade,
always upgrade the workers first, and never downgrade them against an
upgraded catalog.

## Known gaps (planned)

- `TRUNCATE` on a mirrored table stops that table's pump by design, since the
  lake cannot replay it. Recovery is `unregister` plus `register`.
- SIGTERM drain: the worker exits without draining in-flight lake commits.
  This is safe (replay heals on restart) but adds catch-up time after a
  deploy.
- Backup and restore runbook: restoring Postgres and the lake to a mutually
  consistent point is not yet documented.
- Extension upgrades: `ALTER EXTENSION modak UPDATE` is not yet aligned with
  the catalog version stamp. Upgrade the extension and workers together.
