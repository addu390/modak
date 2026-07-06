# TierDB

A fast, transparent and cost-effective way to tier Postgres data into Apache Iceberg. Recent rows stay hot in Postgres, history moves into the lake, and your application keeps running plain SQL against one table as if nothing moved.

Writes stay ACID wherever the row lives, every read is one point-in-time view across both tiers, and failure degrades to lag, never to a wrong answer. With the extension installed, plain SQL reads and writes both tiers in place, and where it cannot be (managed Postgres like RDS), tiering still runs and [connectors](integrations/index.md) cover the cross-tier side.

<video controls muted playsinline style="width: 100%; border-radius: 8px;">
  <source src="https://github.com/user-attachments/assets/25c57f39-c3f9-4c9d-be9c-89891495f6b3" type="video/mp4">
</video>

!!! note "Status: beta"
    TierDB has not cut a stable release. Interfaces can still change. See the note in [Production deployment](operations/production.md) before running it anywhere that matters.

Each table chooses how the two systems share its rows:

- A **tiered** table keeps only its recent partitions in Postgres and moves the rest to Iceberg. This fits append-mostly, time-series data like telemetry and event logs, where old rows should stop taking up Postgres space.
- A **mirrored** table keeps its full copy in Postgres while CDC trails every change into the lake. It can optionally shed heap history it no longer needs hot. This fits reference tables that must stay fully writable in Postgres but should also be visible to the lakehouse.

Either way, a thin open seam stitches the tiers: a monotonic cut-line, a pinned lake snapshot, and a PK-keyed correction delta merged on read. Every query sees a consistent point-in-time view with no duplicates and no gaps.

## Why TierDB

Both tiers stay real, independently usable systems:

- A real Postgres you run OLTP on. TierDB is an extension plus a schema, not a fork or a proxy.
- A real Iceberg warehouse any engine can read. Spark, Trino, and DuckDB see standard Iceberg tables with no TierDB anywhere.

TierDB owns only the glue: routing, tiering, mirroring, and the consistency protocol that makes a two-tier table read like one table.

## What deploys where

| Piece | What it is | Runs |
|-------|------------|------|
| `tierdb` extension | Planner hook (transparent reads), write routers, read pins | Inside your Postgres |
| `tierdb.*` schema | Plain catalog tables, the coordination contract | Inside your database |
| Worker (`tierdb-worker.jar`) | Tiering, CDC mirroring, compaction, maintenance, CLI | Alongside Postgres (VM, k8s, ...) |
| Console (`tierdb-console.jar`) | The worker plus an embedded web console | Optional, instead of the worker |

Postgres itself, the S3-compatible object store, and the optional Iceberg REST catalog are yours. TierDB never hosts them. See [Production deployment](operations/production.md) for the full boundary.

## Start here

- [Quickstart](getting-started/quickstart.md): a full local stack and a scripted walkthrough in about ten minutes.
- [Concepts](getting-started/concepts.md): the five ideas the rest of the docs assume. Table modes, tier key, cut-line, pinned snapshot, delta.
- [Choosing a mode](modes/choosing.md): which mode fits your workload, decided from the shape of the data.
- [The contract](modes/contract.md): the mode-by-mode matrix of what you can read, write, and correct, and which surface does it.
- [Architecture](getting-started/architecture.md): how the extension, the worker, and the catalog cooperate without ever calling each other.
