# Architecture

TierDB is two deployable halves and a contract between them.

```
        ┌─────────────────────────────┐          ┌─────────────────────────┐
        │  Your Postgres              │          │  Worker (JVM daemon)    │
        │  ┌───────────────────────┐  │          │  tiering · mirror pumps │
        │  │ tierdb extension       │  │          │  compaction · verify    │
        │  │ planner hook, routers │  │          │  maintenance · console  │
        │  │ read pins             │  │          └───────────┬─────────────┘
        │  └──────────┬────────────┘  │                      │
        │             │               │                      │
        │      ┌──────▼───────┐       │                      │
        │      │ tierdb.* SQL  │◄──────┼──────────────────────┘
        │      │ catalog      │       │       plain transactions, no RPC
        │      └──────────────┘       │
        └─────────────────────────────┘          ┌─────────────────────────┐
                                                 │  Iceberg warehouse (S3) │
                                                 └─────────────────────────┘
```

## The two halves

The extension (`extension/`, Rust) runs inside the Postgres backend. It owns everything that must happen at query time: the transparent-read planner hook (swapping each registered relation for the two-tier union subquery, Postgres' own view-expansion recipe), the write routers (`tierdb_upsert`/`tierdb_delete`), and read-pin acquire and release. The pure consistency logic (planner, SQL generation, merge rules) lives in a separate crate, `tierdb-core`, with no Postgres dependency, so it unit-tests without a database.

The worker (`worker/`, Java) runs alongside Postgres and owns everything that moves data: tiering (seal, flush, advance, reclaim), the CDC mirror pumps, compaction (folding the delta into equality deletes), initial copies, lake maintenance, and verification. Lake access goes through pluggable ports (`tierdb-lake`) with Iceberg as the shipped implementation. The console binary embeds the same daemon and adds the web UI.

The worker usually runs as its own service, but it does not have to: in [embedded mode](../operations/deployment.md) the extension registers a Postgres background worker that supervises the daemon as a child process, so one Postgres instance carries the whole system.

## The coordination boundary

The halves never call each other. They communicate through the `tierdb.*` catalog tables in your database, and every consistency-critical handoff is one plain Postgres transaction:

- The cut-line advance updates `T`, `S`, and the lake's `metadata_location` atomically, so a reader pins either the old world or the new one, never a mix.
- The compaction publish clears folded delta rows in the same transaction that advances `S`, version-guarded so a row re-corrected mid-fold survives.
- The mirror frontier advance commits only after the lake commit it describes, and replication-slot feedback is sent strictly after that. The slot can only trim WAL the catalog already owns.

This is the single most important structural decision: the `(T, S)` handoff is atomic because Postgres transactions are, and each half can be built, tested, and deployed independently.

## The consistency contract

Reads are correct because four invariants hold:

1. `T` and `S` are monotonic and move together. A regressing advance throws, and the catalog is the arbiter.
2. Reads pin. A query holds `(T, S)` in `tierdb.read_pins` for its transaction, and pins roll back with it.
3. Nothing pinned is mutated. Compaction skips tables with active pins below its target, snapshot expiry never touches the oldest pinned horizon, and partition reclaim waits out pins below the new line.
4. Every data movement is idempotent and journaled. Tiering and compaction record phases in `tierdb.op_log`, and crash recovery replays or adopts (pre-commit gap probes reconcile a lake snapshot the catalog never learned about). Initial copies journal chunks and resume exactly.

Failure of any component therefore degrades to lag, never to a wrong answer. A dead worker stops advancing the seam, and readers keep reading the pinned world.

## Execution

Cold scans run in DuckDB via `pg_duckdb`, reading the pinned `metadata_location` directly (no catalog round-trip on the read path). DuckDB is the vectorized executor of a fully resolved plan. TierDB decides what is current, DuckDB never does. Generated SQL stays executor-portable because `pg_duckdb` may push the whole query down.

## Repository layout

| Path | Contents |
|------|----------|
| `extension/crates/tierdb-core` | Pure domain: planner, SQL generation, merge rules |
| `extension/crates/tierdb-pg` | The pgrx extension: hook, routers, pins, SPI adapters |
| `worker/tierdb-catalog` | Catalog facade over `tierdb.*` (JDBC) |
| `worker/tierdb-cdc` | Logical replication: slots, pgoutput decoding, batching |
| `worker/tierdb-tiering` / `tierdb-compaction` | The data-movement workers |
| `worker/tierdb-lake` / `tierdb-iceberg` | Lake ports and the Iceberg implementation |
| `worker/tierdb-worker` | The headless daemon + CLI |
| `worker/tierdb-console` | The daemon + embedded web console |
| `sql/` | The catalog schema |
