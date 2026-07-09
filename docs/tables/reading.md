# Reading

Every read of a registered table resolves to the same shape: the hot heap above the cut-line unioned with the cold side, which is the pinned Iceberg snapshot merged against the delta, or on a direct table the live lake relation with no overlay. There are three ways to ask for it.

## Transparent reads (default)

With the extension preloaded (`shared_preload_libraries = 'pg_duckdb, tierdb'`), a planner hook rewrites any plain `SELECT` that touches a registered table (predicates, joins, aggregates, CTEs, subqueries) into the two-tier scan, and pins `(T, S)` for the transaction:

```sql
SELECT * FROM public.events WHERE event_time < 100;   -- just works, both tiers
```

The substitution follows Postgres' own view-expansion recipe, so locking and permission checks still target the original relation. DML is never rewritten, and `SELECT ... FOR UPDATE` keeps plain heap semantics.

```sql
SET tierdb.transparent_reads = off;   -- restore raw heap semantics per session
```

The worker daemon always connects with transparent reads off, since it reasons about the physical heap.

## Mirrored tables: heap or hybrid

A mirrored table's heap is complete, so plain scans are untouched by default. Reads cost exactly what they did before TierDB. Sessions can opt into serving the bulk of a scan from the lake instead:

```sql
SET tierdb.mirrored_reads = 'hybrid';
```

A hybrid read first waits (bounded by `tierdb.mirror_wait_ms`, default 5000) for the mirror frontier to pass the session's current WAL position, which proves everything the query's snapshot can see is in the lake. It then splits the union at `max(tier_key) - tierdb.hybrid_lag`. On timeout it falls back to the heap with a `NOTICE`. Mirrored tables registered with retention always read two-tier, because the heap below the retention line is gone.

## The explicit protocol

What the hook does implicitly, you can drive by hand. This is useful for tooling, or for debugging exactly what a query sees:

```sql
BEGIN;
SELECT pin_id FROM tierdb_read_begin('public.events'::regclass) \gset
SELECT tierdb_rewrite_scan('public.events'::regclass) AS scan_sql \gset
-- run :scan_sql, e.g.  SELECT count(*) FROM ( :scan_sql ) q;
SELECT tierdb_read_end(:pin_id);
COMMIT;
```

`tierdb_read_begin` pins `(T, S)` and returns the pin. `tierdb_rewrite_scan` renders the exact union SQL for the pinned view. `tierdb_read_end` releases the pin, and abort releases it automatically, since pins are rows in `tierdb.read_pins` and roll back with the transaction.

The same contract works from outside Postgres. Any engine that can read the catalog and scan Iceberg at a pinned snapshot can produce the identical consistent view. The [seam protocol](../reference/seam.md) page specifies it.

## Execution

The cold branch runs in DuckDB via `pg_duckdb` (`iceberg_scan` on the pinned `metadata_location`). The hot branch is a plain heap scan. DuckDB never decides what is current. TierDB resolves consistency before execution and hands DuckDB a fully specified plan.

## Session GUCs

| GUC | Default | Meaning |
|-----|---------|---------|
| `tierdb.transparent_reads` | `on` | Rewrite `SELECT`s on registered tables to span both tiers |
| `tierdb.mirrored_reads` | `'heap'` | `'hybrid'` opts into two-tier reads on mirrored (no-retention) tables |
| `tierdb.mirror_wait_ms` | `5000` | Bounded wait for the mirror frontier before a hybrid read |
| `tierdb.hybrid_lag` | `0` | Hybrid seam margin, in tier-key units, kept on the heap side |
