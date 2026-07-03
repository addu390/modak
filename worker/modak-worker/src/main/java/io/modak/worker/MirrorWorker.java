package io.modak.worker;

import io.modak.catalog.Catalog;
import io.modak.catalog.RegisteredTable;
import io.modak.cdc.CdcException;
import io.modak.compaction.CompactionWorker;
import io.modak.cdc.ChangeBatch;
import io.modak.cdc.PgOutputMessage;
import io.modak.cdc.ReplicationSource;
import io.modak.cdc.SchemaDivergedException;
import io.modak.common.DeltaRowsBatch;
import io.modak.common.Lsn;
import io.modak.common.RowBatchData;
import io.modak.common.TableId;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.CommittedLakeSnapshot;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeCommitter;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTable;
import io.modak.lake.LakeTieringProps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The mirror pump for one mirrored table: applies DML from the pgoutput slot
 * incrementally, folds batches into the lake, advances the mirror frontier, and
 * only then reports the LSN to the slot. Crash windows heal by idempotent replay.
 */
public final class MirrorWorker implements Runnable {

    private static final long IDLE_SLEEP_MS = 50;
    private static final long CRASH_BACKOFF_MS = 5_000;
    static final int DEFAULT_MAX_BUFFERED_ROWS = 100_000;

    private final Catalog catalog;
    private final LakeStorage lake;
    private final RegisteredTable meta;
    private final String pgUrl;
    private final String pgUser;
    private final String pgPassword;
    private final int batchRows;
    private final long flushIntervalMs;
    private final int maxBufferedRows;
    private final CompactionWorker deltaFold;
    private final long foldIntervalMs;

    private volatile boolean running = true;
    private volatile boolean diverged;
    private Lsn frontier;
    private LakeCommitResult unpublishedFold;
    private long lastFold = System.nanoTime();

    public MirrorWorker(Catalog catalog, LakeStorage lake, RegisteredTable meta,
            String pgUrl, String pgUser, String pgPassword, int batchRows, long flushIntervalMs) {
        this(catalog, lake, meta, pgUrl, pgUser, pgPassword, batchRows, flushIntervalMs,
                DEFAULT_MAX_BUFFERED_ROWS);
    }

    public MirrorWorker(Catalog catalog, LakeStorage lake, RegisteredTable meta,
            String pgUrl, String pgUser, String pgPassword, int batchRows, long flushIntervalMs,
            int maxBufferedRows) {
        this(catalog, lake, meta, pgUrl, pgUser, pgPassword, batchRows, flushIntervalMs,
                maxBufferedRows, null, 0);
    }

    /**
     * With a non-null {@code deltaFold}, the pump also folds {@code modak.delta}
     * corrections into the mirror at quiet points, at most once per
     * {@code foldIntervalMs}. Used for mirrored tables with heap retention, where
     * below-window writes land in the delta because the heap rows are gone.
     */
    public MirrorWorker(Catalog catalog, LakeStorage lake, RegisteredTable meta,
            String pgUrl, String pgUser, String pgPassword, int batchRows, long flushIntervalMs,
            int maxBufferedRows, CompactionWorker deltaFold, long foldIntervalMs) {
        this.catalog = Objects.requireNonNull(catalog);
        this.lake = Objects.requireNonNull(lake);
        this.meta = Objects.requireNonNull(meta);
        this.pgUrl = pgUrl;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
        this.batchRows = batchRows;
        this.flushIntervalMs = flushIntervalMs;
        this.maxBufferedRows = maxBufferedRows;
        this.deltaFold = deltaFold;
        this.foldIntervalMs = foldIntervalMs;
    }

    public void stop() {
        running = false;
    }

    /** True when the pump died on destructive DDL. The daemon must not respawn it. */
    public boolean diverged() {
        return diverged;
    }

    /**
     * Long-lived, streams until {@link #stop()}. Unexpected failures back off
     * and reconnect, except a diverged schema, which replays identically
     * forever and therefore kills this table's pump instead.
     */
    @Override
    public void run() {
        String name = meta.schemaName() + "." + meta.tableName();
        while (running) {
            try {
                pump();
            } catch (SchemaDivergedException e) {
                diverged = true;
                Log.error("%s: mirror pump stopped: %s", name, e.getMessage());
                return;
            } catch (Exception e) {
                if (!running) {
                    return;
                }
                Log.error("%s: mirror pump failed (reconnecting in %dms): %s",
                        name, CRASH_BACKOFF_MS, e);
                sleep(CRASH_BACKOFF_MS);
            }
        }
    }

    void pump() {
        TableId table = meta.id();
        resume();
        frontier = catalog.readMirrorFrontier(table).orElseThrow(() -> new CdcException(
                "no mirror frontier for " + table + ", was the initial copy registered?"));

        try (ReplicationSource source = ReplicationSource.open(
                pgUrl, pgUser, pgPassword, meta.slotName(), meta.publicationName(), Lsn.ZERO)) {
            ChangeBatch batch = new ChangeBatch(
                    table, meta.primaryKeyCols(), meta.tierKeyCol());
            boolean inTx = false;
            boolean skipTxn = false;
            Lsn pending = null;
            long lastFlush = System.nanoTime();

            source.reportFlushed(frontier); // let the slot trim WAL we already hold

            while (running) {
                PgOutputMessage msg = source.poll();
                if (msg == null) {
                    if (due(lastFlush) && !inTx) {
                        if (!batch.isEmpty() || unpublishedFold != null) {
                            if (pending != null) {
                                flush(batch, pending, source);
                                pending = null;
                            }
                        } else if (pending != null) {
                            advanceWithoutData(pending, source);
                            pending = null;
                        } else {
                            advanceOnKeepalive(source);
                        }
                        lastFlush = System.nanoTime();
                    }
                    if (!inTx && pending == null && batch.isEmpty()) {
                        maybeFoldDelta();
                    }
                    sleep(IDLE_SLEEP_MS);
                    continue;
                }
                if (msg instanceof PgOutputMessage.Relation r) {
                    if (r.relationOid() == table.oid()) {
                        List<RowBatchData.Column> added = batch.classify(r);
                        if (!added.isEmpty()) {
                            if (!batch.isEmpty()) {
                                if (!inTx && pending != null) {
                                    flush(batch, pending, source);
                                    pending = null;
                                } else {
                                    foldAtFrontier(batch);
                                }
                            }
                            lakeTable().evolveSchema(added);
                        }
                        batch.onRelation(r);
                    }
                } else if (msg instanceof PgOutputMessage.Begin b) {
                    inTx = true;
                    skipTxn = b.finalLsn().compareTo(frontier) < 0;
                } else if (msg instanceof PgOutputMessage.Insert i) {
                    if (!skipTxn && i.relationOid() == table.oid()) {
                        batch.onInsert(i);
                        relieveMemory(batch);
                    }
                } else if (msg instanceof PgOutputMessage.Update u) {
                    if (!skipTxn && u.relationOid() == table.oid()) {
                        batch.onUpdate(u);
                        relieveMemory(batch);
                    }
                } else if (msg instanceof PgOutputMessage.Delete d) {
                    if (!skipTxn && d.relationOid() == table.oid()) {
                        batch.onDelete(d);
                        relieveMemory(batch);
                    }
                } else if (msg instanceof PgOutputMessage.Commit c) {
                    if (!skipTxn) {
                        pending = c.endLsn();
                    }
                    inTx = false;
                    skipTxn = false;
                    if (pending != null
                            && (batch.size() >= batchRows || unpublishedFold != null
                                    || (due(lastFlush) && !batch.isEmpty()))) {
                        flush(batch, pending, source);
                        pending = null;
                        lastFlush = System.nanoTime();
                    }
                } else if (msg instanceof PgOutputMessage.Truncate) {
                    throw new CdcException("TRUNCATE on mirrored table " + meta.tableName()
                            + " cannot be mirrored, re-register the table");
                }
            }
            if (!inTx && pending != null && !batch.isEmpty()) {
                flush(batch, pending, source);
            }
        }
    }

    /**
     * Folds pending delta corrections into the mirror. Only called at a quiet
     * point (no open transaction, nothing buffered or pending), so the pump
     * stays the table's single lake writer and the publish cannot race a
     * frontier advance.
     */
    private LakeTable lakeTable() {
        return lake.table(new CommitterInitContext(meta.id(), meta.lakeTableRef()),
                new ColdTableSpec(meta.primaryKeyCols(), meta.tierKeyCol()));
    }

    private void maybeFoldDelta() {
        if (deltaFold == null || unpublishedFold != null
                || (System.nanoTime() - lastFold) / 1_000_000 < foldIntervalMs) {
            return;
        }
        lastFold = System.nanoTime();
        try {
            deltaFold.runCycle(meta.id(), Instant.now());
        } catch (Exception e) {
            Log.error("%s.%s: delta fold failed (will retry): %s",
                    meta.schemaName(), meta.tableName(), e);
        }
    }

    private void relieveMemory(ChangeBatch batch) {
        if (batch.size() >= maxBufferedRows) {
            foldAtFrontier(batch);
            Log.info("%s.%s: intermediate fold (transaction exceeds %d buffered row(s))",
                    meta.schemaName(), meta.tableName(), maxBufferedRows);
        }
    }

    private void foldAtFrontier(ChangeBatch batch) {
        TableId table = meta.id();
        DeltaRowsBatch delta = batch.drain();
        try {
            unpublishedFold = lakeTable().mergeWriter()
                    .applyDelta(delta, mirrorProps(table, frontier));
        } catch (Exception e) {
            throw new CdcException("intermediate mirror fold failed for " + table
                    + " (safe: the slot replays into an idempotent fold)", e);
        }
    }

    // A keepalive proves no WAL up to X was withheld, so the frontier can adopt X.
    private void advanceOnKeepalive(ReplicationSource source) {
        Lsn received = source.lastReceived();
        if (received.value() == 0) {
            return;
        }
        advanceWithoutData(received, source);
    }

    private void advanceWithoutData(Lsn lsn, ReplicationSource source) {
        if (lsn.compareTo(frontier) <= 0) {
            return;
        }
        TableId table = meta.id();
        catalog.advanceMirrorFrontier(table, lsn,
                catalog.readCutline(table).snapshot(), Map.of());
        frontier = lsn;
        source.reportFlushed(lsn);
    }

    private void flush(ChangeBatch batch, Lsn lsn, ReplicationSource source) {
        if (lsn == null) {
            throw new IllegalStateException("flush without a committed LSN");
        }
        TableId table = meta.id();
        try {
            LakeCommitResult result;
            if (batch.isEmpty() && unpublishedFold != null) {
                result = unpublishedFold;
            } else {
                result = lakeTable().mergeWriter()
                        .applyDelta(batch.drain(), mirrorProps(table, lsn));
            }
            catalog.advanceMirrorFrontier(table, lsn, result.readable(), result.publishProps());
            unpublishedFold = null;
            frontier = lsn;
            // Feedback strictly last: the slot may only trim WAL the catalog owns.
            source.reportFlushed(lsn);
        } catch (Exception e) {
            throw new CdcException("mirror flush failed for " + table + " at " + lsn
                    + " (safe: the slot replays into an idempotent fold)", e);
        }
    }

    // Crash between lake commit and catalog advance: adopt the lake's own frontier.
    private void resume() {
        TableId table = meta.id();
        try (LakeCommitter<?, ?> committer = lake.tieringFactory()
                .createCommitter(new CommitterInitContext(table, meta.lakeTableRef()))) {
            Optional<CommittedLakeSnapshot> missing = committer.getMissingLakeSnapshot(
                    catalog.readCutline(table).snapshot(), LakeTieringProps.OP_KIND_MIRROR);
            if (missing.isEmpty() || !belongsTo(table, missing.get().snapshotProps())) {
                return;
            }
            String stamped = missing.get().snapshotProps().get(LakeTieringProps.COMMIT_LSN);
            if (stamped == null) {
                return;
            }
            Lsn lakeLsn = new Lsn(Long.parseLong(stamped));
            Optional<Lsn> known = catalog.readMirrorFrontier(table);
            if (known.isEmpty() || lakeLsn.compareTo(known.get()) > 0) {
                catalog.advanceMirrorFrontier(table, lakeLsn, missing.get().readable(),
                        missing.get().publishProps());
                Log.info("%s.%s: mirror frontier backfilled from the lake to %s",
                        meta.schemaName(), meta.tableName(), lakeLsn);
            }
        } catch (Exception e) {
            throw new CdcException("mirror resume probe failed for " + table, e);
        }
    }

    static Map<String, String> mirrorProps(TableId table, Lsn lsn) {
        Map<String, String> props = LakeTieringProps.snapshotProps(UUID.randomUUID(),
                LakeTieringProps.OP_KIND_MIRROR, LakeTieringProps.COMMIT_USER_MIRROR, table);
        props.put(LakeTieringProps.COMMIT_LSN, Long.toString(lsn.value()));
        return props;
    }

    private static boolean belongsTo(TableId table, Map<String, String> snapshotProps) {
        String stamped = snapshotProps.get(LakeTieringProps.TABLE_ID);
        return stamped == null || stamped.equals(Long.toString(table.oid()));
    }

    private boolean due(long lastFlushNanos) {
        return (System.nanoTime() - lastFlushNanos) / 1_000_000 >= flushIntervalMs;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
