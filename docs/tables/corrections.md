# Corrections & schema changes

## Correcting cold rows

Hot rows take plain DML. For rows that only the lake holds, past the cut-line of a tiered table or below the drop boundary of a mirrored table with heap retention, plain `INSERT`, `UPDATE`, and `DELETE` also just work when the extension is installed: inserts route through the spill partition, updates and deletes through a statement rewrite whose cold half is evaluated against the pinned lake scan, all writing the delta overlay with upsert semantics (see the [SQL reference](../reference/sql.md)). The routed functions do the same thing one record at a time, for sessions running with transparent writes off or statement shapes the rewrite rejects:

```sql
-- Upsert: full row image, routed by its tier key.
SELECT tierdb_upsert('public.events'::regclass,
                    '{"id":3,"event_time":110,"val":"corrected"}'::jsonb);

-- Delete: the primary key and the row's tier key.
SELECT tierdb_delete('public.events'::regclass, '3', 110);
```

The router compares the record's tier key against the cut-line. Recent rows go to the heap as ordinary DML, cold rows become `tierdb.delta` entries (upsert or tombstone). Every read merges the delta over the pinned snapshot, newest wins, so a correction is visible immediately, long before it reaches Iceberg.

Fully mirrored tables never need any of this. All writes are plain DML and CDC does the rest.

## Compaction

The worker folds the delta into Iceberg in the background: a worker cycle for tiered tables, the mirror pump between replication batches for mirrored tables with heap retention. Tombstones and overwrites become equality deletes plus data files, committed as one snapshot, and the folded delta rows are cleared in the same transaction that advances the pinned snapshot `S`. Three guards keep this safe:

- The clear is version-guarded, so a row corrected again mid-fold survives the clear and is folded next cycle.
- The pin check runs inside the transaction. An active read pin blocks the publish (the pinned merge still reads those delta rows) and compaction retries next cycle. Folded but unpublished files are cleaned up and re-derived.
- `TIERDB_COMPACTION_BATCH` (default 1000) bounds rows folded per cycle.

A persistently growing backlog (`tierdb_delta_backlog_rows`) usually means a long-held read pin is blocking the fold. See [Operations](../operations/day-2.md#backpressure).

## Schema changes

`ALTER TABLE ... ADD COLUMN` just works, in both modes. The worker adds the column to the Iceberg schema (as an optional field) before the next flush, so old lake rows read as `NULL` and new rows carry the value.

Anything else, such as `DROP COLUMN`, `RENAME COLUMN`, `ALTER TYPE` across type families, or `TRUNCATE` on a mirrored table, cannot be applied to the lake without rewriting history. TierDB fails that table loudly instead of diverging silently:

- Mirrored: the table's pump stops with an error naming the exact difference, and other tables keep mirroring. Recover by re-registering. Run `unregister --table s.t --drop-lake` (the mirror's history is being rewritten anyway), then `register --mode mirrored` for a fresh initial copy.
- Tiered: dropped heap columns simply stop receiving values. Old lake rows keep theirs, new rows read `NULL`. A rename looks like a drop plus add.
