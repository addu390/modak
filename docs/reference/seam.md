# The seam protocol

The consistency seam is not private to the Postgres extension. Everything a reader needs to produce a correct two-tier view lives in plain catalog tables, maintained by the worker. The extension is the reference consumer: it reads the same rows any other engine would. This page specifies the contract so other consumers (Trino, Spark, DuckDB standalone, your own tooling) can implement the same read with the same guarantees.

The catalog DDL is `sql/catalog.sql`, and [Catalog schema](catalog.md) describes each table operationally. This page covers the semantics a consumer must honor.

## The read algorithm

A conforming pinned read of one table:

1. In a single transaction, insert a pin and capture the seam state it pins:

    ```sql
    INSERT INTO tierdb.read_pins
        (table_id, pinned_lake_snapshot_id, pinned_tier_key_hi, expires_at)
    SELECT c.table_id, c.lake_snapshot_id, c.tier_key_hi, now() + interval '15 minutes'
      FROM tierdb.cutline c
     WHERE c.table_id = :table_oid
    RETURNING pin_id, pinned_tier_key_hi AS t, pinned_lake_snapshot_id AS s;

    SELECT lake_props->>'metadata_location' AS metadata_location,
           lake_props->>'snapshot_id'       AS snapshot_id
      FROM tierdb.cutline WHERE table_id = :table_oid;
    ```

    Doing the insert and the read in one transaction is what makes the pin atomic. Read first and pin second, and maintenance can expire S in the gap.

2. Scan the hot branch: the Postgres table with `tier_key >= T`.

3. Scan the cold branch pinned at S. `lake_props` sits on the cut-line row and carries two equivalent handles, published atomically with every advance: `metadata_location` for engines that scan a metadata file directly (DuckDB `iceberg_scan`), and `snapshot_id` for engines that pin through a catalog (Trino `FOR VERSION AS OF`, Spark `VERSION AS OF`). Never read "current", since the current snapshot can be newer than the T you captured.

4. Merge the delta over the cold branch, newest wins: for every `tierdb.delta` row for this table, an `op = 0` row replaces the cold row with the same PK (or adds it if absent), and an `op = 1` row removes it. When several candidates exist for one PK, the largest `version` wins. The hot branch is never merged against the delta.

5. Union hot and merged-cold. The result is a consistent point-in-time view with no duplicates and no gaps.

6. Release the pin (`DELETE FROM tierdb.read_pins WHERE pin_id = :pin`), or let transaction rollback remove it. `expires_at` bounds the damage of a consumer that dies without releasing.

An unpinned read (skip steps 1 and 6, still scan at the captured `metadata_location`) sees a consistent view too, but a long scan races snapshot expiry and file compaction. Pin whenever a scan can outlive the maintenance interval.

## Tier key types

Every tier-key value in the catalog (`tier_key_hi`, `retention_line`, `tierdb.delta.tier_key`, `tierdb.partitions` bounds, pinned values) is a canonical `bigint`. The column's native type is in `tierdb.tables.tier_key_type`, and the codec between the two is fixed and order-preserving:

| `tier_key_type` | Canonical value |
|-----------------|-----------------|
| `bigint` | The value itself |
| `timestamptz` | Microseconds since `1970-01-01 00:00:00+00` |
| `timestamp` | Microseconds since `1970-01-01 00:00:00`, the naive value read as UTC |
| `date` | Days since `1970-01-01` |

A consumer converts at its own boundaries only: encode the native column to canonical when comparing rows against `T` or `retention_line`, and render canonical values back to native literals when pushing predicates into the heap or the lake. Everything between the boundaries stays canonical, so new types extend the registry without touching the protocol.

## The invariants consumers rely on

The worker guarantees all of these, and a consumer may assume them:

- `T` is monotonic per table, and `(T, S, metadata_location)` advance together in one Postgres transaction, never independently.
- At any committed instant: rows with `tier_key >= T` are in the heap, rows below `T` are in the lake at `S` as corrected by the delta. Nothing is in both, nothing is in neither.
- Every `tierdb.delta` row targets a cold row (`tier_key < T`). Compaction folds delta rows into the lake and clears them under a version guard, so a row corrected mid-fold survives with its newest value.
- Rows below `tierdb.cutline.retention_line` (when set) have been expired from the lake. Writers must not create delta rows below it, since retention purges them unfolded, and readers should expect no data there.
- `version` values come from one sequence (`tierdb.delta_version`) and are assignment-ordered. Newest-wins by `version` is always well defined.
- Lake maintenance never expires a snapshot at or above the oldest `pinned_lake_snapshot_id` in `tierdb.read_pins`, and never rewrites data files in a way that changes the content any live snapshot serves.
- `pk` is the canonical text encoding of the primary key: a single-column key is its Postgres text form, a composite key joins the parts with `chr(31)` after escaping `\` and `chr(31)` with `\`.

## Direct tables

A direct table (`mode = 'direct'`) has no pinned snapshot and no delta merge: skip step 4, and in step 3 attach the catalog named by the table's [storage profile](../tables/storage-profiles.md) (`lake_config->>'catalog.uri'`) and scan the current state of `lake_table_ref` with `tier_key < T`. A lake table with no snapshot yet has no cold half, and the read is the heap alone. The pin still protects `T` and file maintenance, but not the lake contents, so a long scan can observe a concurrent commit. A consumer that writes cold rows must hold the shared per-table advisory lock on key `hashtextextended('tierdb_lake_' || table_id::text, 0)` across the lake commit (transaction- or session-scoped, whichever fits its connection model), because concurrent Iceberg commits cannot rebase.

## Mirrored tables

A mirrored table's heap is complete, so the default read is a plain heap scan with no seam involved. The seam state for mirrored tables is the frontier `F` (`tierdb.cutline.replicated_lsn`): everything committed at or below `F` is provably in the lake. A consumer that wants to serve a mirrored read from the lake follows the hybrid recipe: wait until `F` passes the WAL position its snapshot requires, then split at a tier-key point of its choosing. A mirrored table registered with retention has shed heap partitions and reads exactly like a tiered table. It writes like one too: its cut-line sits at the drop boundary, corrections below it are delta rows, and the pump folds them into the mirror.

## Compatibility

The catalog schema is versioned in `tierdb.schema_meta`, and the worker refuses to run against a database newer than itself. Any change to the tables or semantics on this page bumps that version and ships a migration. Consumers should check the version they were written against.

## Consumers

The `tierdb` Postgres extension is the reference consumer, running this protocol inside the planner hook with transaction-scoped pins plus write-side routing. [Spark](../integrations/spark.md) and [Trino](../integrations/trino.md) are the first of the [connectors](../integrations/index.md), which share the protocol layer in `tierdb-connector`. Each consumer is a thin client of this page, not a fork of the engine.

## One vocabulary, two runtimes

The routing policy of this page is implemented twice, once in Rust for the extension and DuckDB (`tierdb-core`) and once in Java for the connectors and the worker, because the two stacks cannot share a runtime. Both express it through the same constructs, name for name:

| Concept | Rust | Java |
|---------|------|------|
| How the table persists (source of write routing) | `Mode` | `io.tierdb.common.mode.Mode` |
| One registered heap relation + its mode | `Table` | (catalog + `TableSeam`) |
| How this query reads that table | `Read::{Heap, Seam}` | `io.tierdb.connector.read.Read` |
| Cold half of a seam read | `Cold::{Delta, Live, Merge}` | `io.tierdb.connector.read.Cold` |
| Which side of the cut-line a row falls on | `RouteTarget` | `RouteTarget` |
| Where a routed cold write lands (delta buffer or lake) | `ColdSink` | `ColdSink` |
| Where one incoming row must land | `plan_insert` → `InsertPlan` | `planInsert` → `InsertPlan` |
| Which tiers one matched row is removed from | `plan_delete` → `DeletePlan` | `planDelete` → `DeletePlan` |
| An update as a delete of the old placement plus an insert of the new | `plan_update` → `UpdatePlan` | `planUpdate` → `UpdatePlan` |

Callers never assemble a read from flags: they call `Table::scan` / `scan_pinned` / `scan_hybrid` (Rust) or `SeamState.scan` / `scanHybrid` (Java). `scan` degrades to a delta-only cold half while no lake snapshot is committed; `scan_pinned` is its strict twin for consumers whose cut-line must always carry committed metadata (the Postgres extension). The write matrices are kept identical by twin conformance suites (`mode.rs` tests and `ModeTest`) that enumerate every mode and target case for case. What differs downstream is only execution: the Rust side renders the plan as SQL through a `SqlDialect`, while the Java side executes it through engine APIs (JDBC batches, Spark jobs, Trino page sources).
