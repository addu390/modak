# Deployment modes

One worker binary, two ways to run it. Both coordinate through the `modak.*` catalog and campaign for the same leader lease, so they coexist freely: exactly one worker leads at a time, the rest stand by.

## External workers (the default)

The daemon runs as its own service, wherever you run services (VM, k8s, ECS, ...), configured entirely through `MODAK_*` env vars pointing at your Postgres and object store:

```bash
MODAK_PG_URL='jdbc:postgresql://db.internal:5432/app?sslmode=verify-full'
MODAK_PG_USER=modak_worker
MODAK_WAREHOUSE=s3://analytics-lake/modak
java -jar modak-worker.jar run
```

This is the disaggregated topology: the data-moving half scales, restarts, and upgrades on its own schedule, without touching the database. Run at least two instances; they fail over automatically through the leader lease, including the mirrored tables' replication slots. And because the worker is just a JDBC client, it runs against managed Postgres (RDS, Cloud SQL) where extensions are forbidden, with reads served through [connectors](../integrations/index.md).

[Production deployment](production.md) documents this mode end to end: roles, encryption, WAL safety, upgrades.

## Embedded worker

The extension registers a Postgres background worker that launches and supervises the same daemon as a child process. One Postgres instance with the extension installed is a fully working Modak, web console included; there is no second service to deploy, monitor, or restart.

The supervisor starts the daemon after recovery finishes (never on a hot standby), restarts it if it exits, forwards Postgres shutdown to it, and sends its output to the Postgres server log. After a failover, the promoted primary starts its own.

No Java install is needed: releases ship a `modak-embedded` bundle, the console jar plus a trimmed self-contained runtime. Unpack it to `/opt/modak` and the supervisor finds it there by default.

## Choosing

- Production fleets and anything that should scale or upgrade independently of the database: external.
- Managed Postgres where you cannot install extensions: external, it is the only option.
- Development machines, demos, small single-instance deployments, hosting platforms where you control the Postgres image but not a fleet: embedded.
- Mixed: enabling embedded next to an external fleet is safe. The lease arbitrates, and the embedded worker simply stands by unless it wins.

## Running embedded

```ini
shared_preload_libraries = 'pg_duckdb, modak'
modak.embedded_worker = on
```

Both need a Postgres restart. If the GUC is on but the library is not preloaded, startup logs a warning and nothing runs.

| GUC | Default | Meaning |
|-----|---------|---------|
| `modak.embedded_worker` | `off` | Register the supervisor background worker. |
| `modak.worker_command` | unset | Command line to run. Unset, the bundle under `/opt/modak` is used when present. |
| `modak.worker_database` | `postgres` | Database the daemon connects to. |
| `modak.worker_env` | unset | Extra environment as `key=value;key=value`. |

The daemon reads the same `MODAK_*` env vars as an external worker. It inherits the postmaster's environment, the supervisor sets `MODAK_PG_URL` to this instance, and `modak.worker_env` entries are applied last and win:

```ini
modak.worker_env = 'MODAK_PG_USER=modak_worker;MODAK_WAREHOUSE=s3://lake/modak'
```

A custom command replaces the bundled default:

```ini
modak.worker_command = 'java -jar /path/to/modak-console.jar run'
```

The repository ships a compose overlay that runs the example stack embedded, with no worker container:

```bash
EXAMPLE_EMBEDDED=1 make -C example up
EXAMPLE_EMBEDDED=1 make -C example scenarios
```
