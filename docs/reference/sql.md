# SQL API

Everything the `tierdb` extension exposes in SQL.

## Transparent writes

On tiered tables (and mirrored tables with heap retention), plain `INSERT`, `UPDATE`, and `DELETE` work for any tier key.

Inserts route row by row. Rows at or above the cut-line take the normal heap path, untouched. Rows below it have no heap partition, so they land in a DEFAULT partition the extension attaches (the spill, `<table>_tierdb_spill`) whose trigger writes them to `tierdb.delta` instead. The spill stays empty, the command tag counts both halves, and the whole statement is one transaction. `COPY` routes the same way.

Updates and deletes route by statement. A WHERE clause that provably stays at or above the cut-line (`WHERE event_time >= <recent>`, an equality or IN list above it) passes through untouched, so OLTP statements keep their exact plans and full feature set. Anything that may reach cold rows is rewritten at plan time into two halves: the original statement bounded to the heap, plus a delta write evaluated against the pinned cold scan. `SET v = v + 1` computes from the old cold image, `UPDATE n` counts both halves, `RETURNING` yields hot and cold rows alike, and everything commits or rolls back as one transaction. The cold half reads through pg_duckdb, same as transparent reads.

Setting the tier key moves the row. A cold row moving within the cold tier becomes a delta entry that remembers its old partition so the fold cleans it up. A cold row moving above the cut-line becomes a tombstone plus a heap row, and a hot row moving below it re-routes through the spill. Either way the row exists exactly once afterwards.

The registrar enables this automatically when the extension is installed. The remaining caveats are loud errors rather than silent surprises: an `INSERT` of a cold row rejects `RETURNING` and `ON CONFLICT`, updates and deletes that may touch cold rows reject `UPDATE ... FROM` / `DELETE ... USING`, `WITH`, and `SET` on a primary key column, and rows below the lake retention line are rejected outright.

### `tierdb_enable_transparent_writes(table regclass) → text`

Attaches the spill partition and trigger. Idempotent. Refuses fully mirrored tables, whose heap takes every insert directly. UPDATE/DELETE transparency needs no per-table setup, the planner hook covers every registered tiered table.

### `tierdb_disable_transparent_writes(table regclass) → text`

Drops the spill. Cold inserts then fail partition routing, the plain Postgres behavior.

## Functions

### `tierdb_upsert(table regclass, row jsonb) → text`

Routes one record by its tier key vs the cut-line. Recent rows become plain heap DML, cold rows become a `tierdb.delta` upsert entry. `row` is the full row image, keys matching column names. Returns the route taken.

```sql
SELECT tierdb_upsert('public.events'::regclass,
                    '{"id":3,"event_time":110,"val":"C!"}'::jsonb);
```

### `tierdb_delete(table regclass, key jsonb, tier_key bigint) → text`

Routes one delete. `key` is the primary key, a bare value for a single-column key or an object for composite keys. Cold deletes become delta tombstones. The key fields are kept as the payload because the compaction fold needs their typed values. Overloads accept the tier key in its native type (`timestamptz`, `timestamp`, `date`) for tables with temporal tier keys.

```sql
SELECT tierdb_delete('public.events'::regclass, '1', 10);
SELECT tierdb_delete('public.fleet'::regclass, '{"tenant_id":2,"vehicle_id":7}', 90);
SELECT tierdb_delete('public.readings'::regclass, '5', TIMESTAMPTZ '2026-01-03 08:00:00+00');
```

### `tierdb_read_begin(table regclass) → (pin_id bigint, ...)`

Pins the table's current `(T, S)` for this transaction and returns the pin. The pin is a row in `tierdb.read_pins` and rolls back with the transaction, so a crashed client cannot leak one.

### `tierdb_rewrite_scan(table regclass) → text`

Renders the exact two-tier union SQL for the pinned view, the same query the transparent-read hook substitutes. Run it inline (`FROM ( :scan_sql ) q`) or inspect it.

### `tierdb_read_end(pin_id bigint)`

Releases a pin before commit. Optional, since commit and abort clean up regardless, but polite for long transactions.

### `tierdb_explain(sql text) → setof text`

The routing report. Analyzes a statement, runs the same classification the hooks run against the live catalog, and reports where rows will come from or go to, without executing anything. Works for `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `COPY ... FROM`.

```sql
SELECT tierdb_explain('UPDATE public.events SET val = ''x'' WHERE id = 42');
```

```text
UPDATE on public.events (tiered): cut-line T=200
  verdict: may touch both tiers, rewritten into two halves
  hot half: the original UPDATE bounded to event_time >= 200 on the heap
  cold half: matching lake rows become tierdb.delta entries, visible
  immediately, folded into iceberg by the worker
```

For inserts with literal `VALUES`, the report counts rows per destination. Statement shapes the rewrite would refuse show up as `rejected:` lines instead of errors. The report reflects the cut-line at call time, so the same statement can route differently after tiering moves it.

### `tierdb_version() → text`

The installed extension version.

## Session GUCs

| GUC | Default | Meaning |
|-----|---------|---------|
| `tierdb.transparent_reads` | `on` | Rewrite `SELECT`s on registered tables into the two-tier union scan, pinning `(T, S)` for the transaction |
| `tierdb.transparent_writes` | `on` | Route below-cut-line writes to `tierdb.delta`: inserts through the spill partition, UPDATE/DELETE through the statement rewrite. When `off`, cold inserts raise the plain missing-partition error and UPDATE/DELETE quietly match heap rows only, as without the extension |
| `tierdb.mirrored_reads` | `'heap'` | Read mode for mirrored tables without retention. `'heap'` leaves plain scans untouched, `'hybrid'` serves the bulk from the lake |
| `tierdb.mirror_wait_ms` | `5000` | Bounded wait (ms) for the mirror frontier before a hybrid read. On timeout the query falls back to the heap with a `NOTICE` |
| `tierdb.hybrid_lag` | `0` | Hybrid seam margin in tier-key units. The union splits at `max(tier_key) - lag` |
| `tierdb.explain` | `off` | Raise a `NOTICE` for every routing decision: two-tier reads, DML rewrites and passthroughs, and cold rows written to `tierdb.delta`. `tierdb_explain()` gives the same report without running the statement |

## Views

`tierdb.status` has one row per registered table: mode, cut-line `T` and pinned snapshot `S`, mirror frontier, delta backlog, active read pins, whether an initial copy is in flight, and partition state counts. See the [catalog reference](catalog.md).
