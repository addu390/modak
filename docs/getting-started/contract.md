# The contract

A consumer does not care how Modak places data. They care about one promise:
ingest data anytime, anywhere on the time axis, and update or delete it the
same way, whether it landed yesterday or two years ago. This page states that
promise per mode and names the path each operation takes. Everything here
follows from two facts: the heap takes plain DML, and anything only the lake
holds goes through `modak.delta` or bulk ingest.

## What each mode stores

| | Postgres holds | Iceberg holds | Overlap |
|---|---|---|---|
| Tiered | recent partitions (`tier_key >= T`) | everything below `T`, minus lake retention | none, the cut-line splits exactly |
| Fully mirrored | everything | trailing full copy (frontier `F`) | total, the lake trails by the CDC lag |
| Mirrored + heap retention | a bounded window | full history | the window, once mirrored |

## Operations

Every cell is a supported path today.

| Operation | Tiered | Fully mirrored | Mirrored + heap retention |
|---|---|---|---|
| Insert, recent | plain `INSERT` | plain `INSERT` | plain `INSERT` |
| Insert, historical | plain `INSERT` (extension) or `modak_upsert()` or a connector, to the delta | plain `INSERT` | same as tiered |
| Update, recent | plain `UPDATE` | plain `UPDATE` | plain `UPDATE` |
| Update, historical | plain `UPDATE` (extension) or `modak_upsert()` or a connector, to the delta | plain `UPDATE` | same as tiered |
| Delete, recent | plain `DELETE` | plain `DELETE` | plain `DELETE` |
| Delete, historical | plain `DELETE` (extension) or `modak_delete()` or a connector, tombstone to the delta | plain `DELETE` | same as tiered |
| Bulk historical load | `modak-worker ingest` (Parquet or records), upsert semantics | plain `COPY` | `modak-worker ingest` |
| Read | one seam-split view | heap, or opt-in hybrid | one seam-split view |

"Historical" means rows the heap no longer holds: below `T` on a tiered table,
below the drop boundary with heap retention. On a fully mirrored table the
heap holds everything, so the distinction disappears and every write is plain
DML.

The one hard boundary is explicit expiry. A tiered table with
`--lake-retention` has deleted rows below the retention line `R` from the lake
on purpose, and every write path rejects rows below it rather than silently
resurrecting data the policy removed.

## API surfaces

| Capability | SQL + extension | SQL, no extension | Spark | CLI |
|---|---|---|---|---|
| Consistent two-tier read | transparent, any query | recent data only | `ModakSpark.read` | - |
| Routed insert | transparent, plain `INSERT` or `COPY` | recent data only | `ModakSpark.write` | - |
| Routed update / delete | transparent, plain `UPDATE` / `DELETE` | recent data only | `ModakSpark.write` / `ModakSpark.delete` | - |
| Bulk historical load | - | - | - | `modak-worker ingest` |

Without the extension, plain SQL still covers everything on a fully mirrored
table and all recent data elsewhere. Historical writes need one of the routed
surfaces. The [seam protocol](../reference/seam.md) is public, so any engine
can implement what Spark implements.

Transparent DML has a short list of shapes it rejects loudly instead of
routing, such as `INSERT ... RETURNING` for cold rows and `UPDATE ... FROM`
that may reach them. The [SQL reference](../reference/sql.md#transparent-writes)
lists them all.

To see which path a specific statement takes without running it, pass it to
[`modak_explain`](../reference/sql.md#modak_explainsql-text-setof-text). The
console's SQL playground has an Explain button that shows the same report, and
`SET modak.explain = on` makes a session narrate every routing decision as a
`NOTICE`.

## One write semantic for cold data

Every historical write path, row-at-a-time delta or bulk ingest, has the same
meaning: upsert by primary key, newest wins, deletes as tombstones. The two
paths differ only in shape and visibility. The delta is for row-scale
corrections and is readable the moment it commits. Bulk ingest is for volume,
readable when the pinned snapshot advances. Nobody chooses between "importing"
and "correcting". Re-ingesting a corrected batch is just ingesting it.

## Who folds the delta

Delta rows are visible to every reader the moment they commit, and the fold
into Iceberg is a background concern. On tiered tables the worker folds each cycle.
On mirrored tables with heap retention the mirror pump folds between
replication batches, keeping one writer per lake table. Fully mirrored tables
have nothing to fold.
