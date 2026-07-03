# The seam protocol

The consistency seam is not private to the Postgres extension. Everything a
reader needs to produce a correct two-tier view lives in plain catalog tables,
maintained by the worker. The extension is the reference consumer: it reads
the same rows any other engine would. This page specifies the contract so
other consumers (Trino, Spark, DuckDB standalone, your own tooling) can
implement the same read with the same guarantees.

The catalog DDL is `sql/catalog.sql`, and
[Catalog schema](catalog.md) describes each table operationally. This page
covers the semantics a consumer must honor.

## The read algorithm

A conforming pinned read of one table:

1. In a single transaction, insert a pin and capture the seam state it pins:

    ```sql
    INSERT INTO modak.read_pins
        (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at)
    SELECT c.table_id, c.lake_snapshot_id, c.tier_key_hi, now() + interval '15 minutes'
      FROM modak.cutline c
     WHERE c.table_id = :table_oid
    RETURNING pin_id, pinned_tier_key_hi AS t, pinned_lake_snapshot_id AS s;

    SELECT lake_props->>'metadata_location' AS metadata_location,
           lake_props->>'snapshot_id'       AS snapshot_id
      FROM modak.tables WHERE table_id = :table_oid;
    ```

    Doing the insert and the read in one transaction is what makes the pin
    atomic. Read first and pin second, and maintenance can expire S in the
    gap.

2. Scan the hot branch: the Postgres table with `tier_key >= T`.

3. Scan the cold branch pinned at S. `lake_props` carries two equivalent
   handles, published atomically with every advance: `metadata_location` for
   engines that scan a metadata file directly (DuckDB `iceberg_scan`), and
   `snapshot_id` for engines that pin through a catalog (Trino
   `FOR VERSION AS OF`, Spark `VERSION AS OF`). Never read "current", since
   the current snapshot can be newer than the T you captured.

4. Merge the delta over the cold branch, newest wins:
   for every `modak.delta` row for this table, an `op = 0` row replaces the
   cold row with the same PK (or adds it if absent), and an `op = 1` row
   removes it. When several candidates exist for one PK, the largest
   `version` wins. The hot branch is never merged against the delta.

5. Union hot and merged-cold. The result is a consistent point-in-time view
   with no duplicates and no gaps.

6. Release the pin (`DELETE FROM modak.read_pins WHERE pin_id = :pin`), or
   let transaction rollback remove it. `expires_at` bounds the damage of a
   consumer that dies without releasing.

An unpinned read (skip steps 1 and 6, still scan at the captured
`metadata_location`) sees a consistent view too, but a long scan races
snapshot expiry and file compaction. Pin whenever a scan can outlive the
maintenance interval.

## The invariants consumers rely on

The worker guarantees all of these, and a consumer may assume them:

- `T` is monotonic per table, and `(T, S, metadata_location)` advance
  together in one Postgres transaction, never independently.
- At any committed instant: rows with `tier_key >= T` are in the heap, rows
  below `T` are in the lake at `S` as corrected by the delta. Nothing is in
  both, nothing is in neither.
- Every `modak.delta` row targets a cold row (`tier_key < T`). Compaction
  folds delta rows into the lake and clears them under a version guard, so a
  row corrected mid-fold survives with its newest value.
- Rows below `modak.cutline.retention_line` (when set) have been expired from
  the lake. Writers must not create delta rows below it, since retention
  purges them unfolded, and readers should expect no data there.
- `version` values come from one sequence (`modak.delta_version`) and are
  assignment-ordered. Newest-wins by `version` is always well defined.
- Lake maintenance never expires a snapshot at or above the oldest
  `pinned_lake_snapshot_id` in `modak.read_pins`, and never rewrites data
  files in a way that changes the content any live snapshot serves.
- `pk` is the canonical text encoding of the primary key: a single-column key
  is its Postgres text form, a composite key joins the parts with `chr(31)`
  after escaping `\` and `chr(31)` with `\`.

## Mirrored tables

A mirrored table's heap is complete, so the default read is a plain heap scan
with no seam involved. The seam state for mirrored tables is the frontier
`F` (`modak.cutline.replicated_lsn`): everything committed at or below `F` is
provably in the lake. A consumer that wants to serve a mirrored read from the
lake follows the hybrid recipe: wait until `F` passes the WAL position its
snapshot requires, then split at a tier-key point of its choosing.
A mirrored table registered with retention has shed heap partitions and reads
exactly like a tiered table.

## Compatibility

The catalog schema is versioned in `modak.schema_meta`, and the worker
refuses to run against a database newer than itself. Any change to the
tables or semantics on this page bumps that version and ships a migration.
Consumers should check the version they were written against.

## Consumers

Today: the `modak` Postgres extension, the premium consumer. It runs this
protocol inside the planner hook with transaction-scoped pins, plus
write-side routing. And [Spark](../connectors/spark.md), the first of the
[connectors](../connectors/index.md), which share the protocol layer in
`modak-connector`. Each consumer is a thin client of this page, not a fork
of the engine.
