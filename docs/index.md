# Modak

Modak makes a Postgres table and an Apache Iceberg table behave as one table.

Recent rows live in Postgres, history lives in the lake, and plain SQL works
against the whole timeline: `SELECT`, `INSERT`, `UPDATE`, and `DELETE` reach
any row, wherever it lives, with transactional-grade consistency.

!!! note "Status: beta"
    Modak has not cut a stable release. Interfaces can still change. See the
    note in [Production deployment](guides/production.md) before running it
    anywhere that matters.

Each table chooses how the two systems share its rows:

- A **tiered** table keeps only its recent partitions in Postgres and moves the
  rest to Iceberg. This fits append-mostly, time-series data like telemetry and
  event logs, where old rows should stop taking up Postgres space.
- A **mirrored** table keeps its full copy in Postgres while CDC trails every
  change into the lake. It can optionally shed heap history it no longer needs
  hot. This fits reference tables that must stay fully writable in Postgres but
  should also be visible to the lakehouse.

Either way, a thin open seam stitches the tiers: a monotonic cut-line, a pinned
lake snapshot, and a PK-keyed correction delta merged on read. Every query sees
a consistent point-in-time view with no duplicates and no gaps.

## Why Modak

Both tiers stay real, independently usable systems:

- A real Postgres you run OLTP on. Modak is an extension plus a schema, not a
  fork or a proxy.
- A real Iceberg warehouse any engine can read. Spark, Trino, and DuckDB see
  standard Iceberg tables with no Modak anywhere.

Modak owns only the glue: routing, tiering, mirroring, and the consistency
protocol that makes a two-tier table read like one table.

## What deploys where

| Piece | What it is | Runs |
|-------|------------|------|
| `modak` extension | Planner hook (transparent reads), write routers, read pins | Inside your Postgres |
| `modak.*` schema | Plain catalog tables, the coordination contract | Inside your database |
| Worker (`modak-worker.jar`) | Tiering, CDC mirroring, compaction, maintenance, CLI | Alongside Postgres (VM, k8s, ...) |
| Console (`modak-console.jar`) | The worker plus an embedded web console | Optional, instead of the worker |

Postgres itself, the S3-compatible object store, and the optional Iceberg REST
catalog are yours. Modak never hosts them. See
[Production deployment](guides/production.md) for the full boundary.

## Start here

- [Quickstart](getting-started/quickstart.md): a full local stack and a scripted
  walkthrough in about ten minutes.
- [Concepts](getting-started/concepts.md): the five ideas the rest of the docs
  assume. Table modes, tier key, cut-line, pinned snapshot, delta.
- [The contract](getting-started/contract.md): the mode-by-mode matrix of what
  you can read, write, and correct, and which surface does it.
- [Architecture](architecture.md): how the extension, the worker, and the
  catalog cooperate without ever calling each other.
