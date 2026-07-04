package io.modak.catalog;

import io.modak.common.Cutline;
import io.modak.common.DeltaBatch;
import io.modak.common.LakeSnapshotId;
import io.modak.common.Lsn;
import io.modak.common.OpKind;
import io.modak.common.OpPhase;
import io.modak.common.PartitionBounds;
import io.modak.common.PartitionId;
import io.modak.common.PartitionState;
import io.modak.common.TableId;
import io.modak.common.TierKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory {@link Catalog} for unit-testing workers without a database.
 * Mirrors the invariants the JDBC adapter enforces via SQL (unique
 * registration, monotonic {@code (T, S)} advance, legal partition steps).
 */
public final class InMemoryCatalog implements Catalog {

    private record Pin(TableId table, Cutline at) {}

    private final Map<TableId, RegisteredTable> tables = new ConcurrentHashMap<>();
    private final Map<String, TableId> byName = new ConcurrentHashMap<>();
    private final Map<TableId, Cutline> cutlines = new ConcurrentHashMap<>();
    private final Map<TableId, TierKey> retentionLines = new ConcurrentHashMap<>();
    private final Map<TableId, Lsn> frontiers = new ConcurrentHashMap<>();
    private final Map<PartitionId, PartitionInfo> partitions = new ConcurrentHashMap<>();
    private final Map<Long, Pin> pins = new ConcurrentHashMap<>();
    private final Map<UUID, TieringOp> ops = new ConcurrentHashMap<>();
    private final List<UUID> opOrder = new ArrayList<>();
    private final Map<String, LoadLabel> loadLabels = new ConcurrentHashMap<>();
    private final List<String> loadOrder = new ArrayList<>();
    private final AtomicLong pinSeq = new AtomicLong();
    private final List<DeltaBatch.Key> clearedDeltaKeys = new ArrayList<>();

    private static String nameKey(String schema, String table) {
        return schema + "." + table;
    }

    @Override
    public TableId register(TableRegistration r) {
        String key = nameKey(r.schemaName(), r.tableName());
        if (byName.containsKey(key)) {
            throw new CatalogException("table already registered: " + key);
        }
        TableId id = new TableId(r.oid());
        tables.put(id, new RegisteredTable(
                id, r.schemaName(), r.tableName(), r.primaryKeyCols(), r.tierKeyCol(),
                r.partitionScheme(), r.lakeFormat(), r.lakeTableRef(), r.lakeProps(), 1,
                r.mode(), r.publicationName(), r.slotName(), r.heapRetentionLag(),
                r.lakeRetentionLag()));
        byName.put(key, id);
        return id;
    }

    @Override
    public synchronized boolean unregister(TableId table) {
        RegisteredTable removed = tables.remove(table);
        if (removed == null) {
            return false;
        }
        byName.remove(nameKey(removed.schemaName(), removed.tableName()));
        cutlines.remove(table);
        retentionLines.remove(table);
        frontiers.remove(table);
        partitions.keySet().removeIf(p -> p.table().equals(table));
        pins.values().removeIf(p -> p.table().equals(table));
        loadLabels.values().removeIf(l -> l.table().equals(table));
        loadOrder.removeIf(k -> !loadLabels.containsKey(k));
        return true;
    }

    @Override
    public Optional<RegisteredTable> lookup(String schemaName, String tableName) {
        TableId id = byName.get(nameKey(schemaName, tableName));
        return id == null ? Optional.empty() : Optional.ofNullable(tables.get(id));
    }

    @Override
    public Optional<RegisteredTable> get(TableId table) {
        return Optional.ofNullable(tables.get(table));
    }

    @Override
    public List<RegisteredTable> listTables() {
        return new ArrayList<>(tables.values());
    }

    @Override
    public void initCutline(TableId table, TierKey t, LakeSnapshotId snapshot) {
        requireTable(table);
        cutlines.put(table, new Cutline(t, snapshot));
    }

    @Override
    public Cutline readCutline(TableId table) {
        Cutline c = cutlines.get(table);
        if (c == null) {
            throw new CatalogException("no cut-line for table " + table);
        }
        return c;
    }

    @Override
    public synchronized void advanceCutline(TableId table, TierKey newT, LakeSnapshotId snapshot) {
        Cutline cur = readCutline(table);
        if (newT.compareTo(cur.t()) < 0 || snapshot.compareTo(cur.snapshot()) < 0) {
            throw new CatalogException("cut-line must advance monotonically: "
                    + cur + " -> (" + newT + ", " + snapshot + ")");
        }
        cutlines.put(table, new Cutline(newT, snapshot));
    }

    @Override
    public synchronized void advanceCutline(TableId table, TierKey newT, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch) {
        advanceCutline(table, newT, snapshot);
        patchLakeProps(table, lakePropsPatch);
    }

    @Override
    public synchronized void advanceRetentionLine(TableId table, TierKey newT) {
        Cutline cur = readCutline(table);
        if (newT.compareTo(cur.t()) < 0) {
            throw new CatalogException("retention line must advance monotonically: "
                    + cur.t() + " -> " + newT);
        }
        cutlines.put(table, new Cutline(newT, cur.snapshot()));
    }

    @Override
    public Optional<Lsn> readMirrorFrontier(TableId table) {
        return Optional.ofNullable(frontiers.get(table));
    }

    @Override
    public synchronized void advanceMirrorFrontier(TableId table, Lsn lsn, LakeSnapshotId snapshot,
            Map<String, String> lakePropsPatch) {
        Cutline cur = readCutline(table);
        Lsn frontier = frontiers.get(table);
        if ((frontier != null && lsn.compareTo(frontier) < 0)
                || snapshot.compareTo(cur.snapshot()) < 0) {
            throw new CatalogException("mirror frontier must advance monotonically: ("
                    + frontier + ", " + cur.snapshot() + ") -> (" + lsn + ", " + snapshot + ")");
        }
        frontiers.put(table, lsn);
        cutlines.put(table, new Cutline(cur.t(), snapshot));
        patchLakeProps(table, lakePropsPatch);
    }

    private void patchLakeProps(TableId table, Map<String, String> lakePropsPatch) {
        if (lakePropsPatch.isEmpty()) {
            return;
        }
        RegisteredTable cur = tables.get(table);
        tables.put(table, new RegisteredTable(
                cur.id(), cur.schemaName(), cur.tableName(), cur.primaryKeyCols(),
                cur.tierKeyCol(), cur.partitionScheme(), cur.lakeFormat(), cur.lakeTableRef(),
                JdbcCatalog.jsonObject(lakePropsPatch), cur.schemaVersion(),
                cur.mode(), cur.publicationName(), cur.slotName(), cur.heapRetentionLag(),
                cur.lakeRetentionLag()));
    }

    @Override
    public synchronized void publishCompaction(TableId table, LakeSnapshotId snapshot, DeltaBatch folded,
            Map<String, String> lakePropsPatch) {
        if (pinnedHorizon(table).isPresent()) {
            throw new CatalogException("publishCompaction blocked by active read pins for " + table);
        }
        Cutline cur = readCutline(table);
        if (snapshot.compareTo(cur.snapshot()) < 0) {
            throw new CatalogException("compaction snapshot must not regress: "
                    + cur.snapshot() + " -> " + snapshot);
        }
        cutlines.put(table, new Cutline(cur.t(), snapshot));
        clearedDeltaKeys.addAll(folded.keys());
        patchLakeProps(table, lakePropsPatch);
    }

    @Override
    public synchronized void publishRetention(TableId table, LakeSnapshotId snapshot,
            TierKey below, Map<String, String> lakePropsPatch) {
        if (pinnedHorizon(table).isPresent()) {
            throw new CatalogException("publishRetention blocked by active read pins for " + table);
        }
        Cutline cur = readCutline(table);
        if (snapshot != null) {
            if (snapshot.compareTo(cur.snapshot()) < 0) {
                throw new CatalogException("retention snapshot must not regress: "
                        + cur.snapshot() + " -> " + snapshot);
            }
            cutlines.put(table, new Cutline(cur.t(), snapshot));
        }
        TierKey line = retentionLines.get(table);
        if (line != null && below.compareTo(line) < 0) {
            throw new CatalogException("retention line must advance monotonically: "
                    + line + " -> " + below);
        }
        retentionLines.put(table, below);
        partitions.entrySet().removeIf(e -> e.getKey().table().equals(table)
                && e.getValue().state() == PartitionState.DROPPED
                && e.getValue().bounds().hi().compareTo(below) <= 0);
        patchLakeProps(table, lakePropsPatch);
    }

    @Override
    public Optional<TierKey> readRetentionLine(TableId table) {
        return Optional.ofNullable(retentionLines.get(table));
    }

    @Override
    public Cutline readHorizon(TableId table) {
        return pinnedHorizon(table).orElseGet(() -> readCutline(table));
    }

    @Override
    public Optional<Cutline> pinnedHorizon(TableId table) {
        List<Cutline> held = pins.values().stream()
                .filter(p -> p.table().equals(table))
                .map(Pin::at)
                .toList();
        if (held.isEmpty()) {
            return Optional.empty();
        }
        TierKey minT = held.stream().map(Cutline::t).min(TierKey::compareTo).orElseThrow();
        LakeSnapshotId minS =
                held.stream().map(Cutline::snapshot).min(LakeSnapshotId::compareTo).orElseThrow();
        return Optional.of(new Cutline(minT, minS));
    }

    private static String loadKey(TableId table, String label) {
        return table.oid() + "|" + label;
    }

    @Override
    public synchronized boolean beginLoad(TableId table, String label, LoadState state,
            String stagedFilesJson, String resultJson) {
        requireTable(table);
        String key = loadKey(table, label);
        if (loadLabels.containsKey(key)) {
            return false;
        }
        loadLabels.put(key, new LoadLabel(table, label, state, stagedFilesJson, resultJson));
        loadOrder.add(key);
        return true;
    }

    @Override
    public Optional<LoadLabel> lookupLoad(TableId table, String label) {
        return Optional.ofNullable(loadLabels.get(loadKey(table, label)));
    }

    @Override
    public synchronized List<LoadLabel> stagedLoads(TableId table) {
        List<LoadLabel> out = new ArrayList<>();
        for (String key : loadOrder) {
            LoadLabel l = loadLabels.get(key);
            if (l != null && l.table().equals(table) && l.state() == LoadState.STAGED) {
                out.add(l);
            }
        }
        return out;
    }

    @Override
    public synchronized void finishLoad(TableId table, List<String> labels,
            LakeSnapshotId snapshot, Map<String, String> lakePropsPatch) {
        Cutline cur = readCutline(table);
        if (snapshot.compareTo(cur.snapshot()) > 0) {
            cutlines.put(table, new Cutline(cur.t(), snapshot));
        }
        for (String label : labels) {
            String key = loadKey(table, label);
            LoadLabel l = loadLabels.get(key);
            if (l != null && l.state() == LoadState.STAGED) {
                loadLabels.put(key, new LoadLabel(table, label, LoadState.COMMITTED,
                        l.stagedFilesJson(), l.resultJson()));
            }
        }
        patchLakeProps(table, lakePropsPatch);
    }

    @Override
    public synchronized void logOpPhase(UUID opId, TableId table, OpKind opKind, OpPhase phase,
            LakeSnapshotId snapshot, String detailsJson) {
        TieringOp prev = ops.get(opId);
        if (prev == null) {
            opOrder.add(opId);
        }
        Optional<LakeSnapshotId> snap = snapshot != null
                ? Optional.of(snapshot)
                : (prev != null ? prev.snapshot() : Optional.empty());
        String details = detailsJson != null ? detailsJson : (prev != null ? prev.detailsJson() : null);
        ops.put(opId, new TieringOp(opId, table, opKind, phase, snap, details));
    }

    @Override
    public synchronized List<TieringOp> findIncompleteOps(TableId table, OpKind opKind) {
        List<TieringOp> out = new ArrayList<>();
        for (UUID id : opOrder) {
            TieringOp op = ops.get(id);
            if (op.table().equals(table) && op.opKind() == opKind
                    && !op.phase().isTerminal()) {
                out.add(op);
            }
        }
        return out;
    }

    @Override
    public void upsertPartition(PartitionId id, PartitionBounds bounds, PartitionState state) {
        requireTable(id.table());
        partitions.put(id, new PartitionInfo(id, bounds, state));
    }

    @Override
    public List<PartitionInfo> listPartitions(TableId table) {
        List<PartitionInfo> out = new ArrayList<>();
        for (PartitionInfo p : partitions.values()) {
            if (p.id().table().equals(table)) {
                out.add(p);
            }
        }
        return out;
    }

    @Override
    public synchronized void transition(PartitionId partition, PartitionState from, PartitionState to) {
        PartitionInfo cur = partitions.get(partition);
        if (cur == null) {
            throw new CatalogException("unknown partition: " + partition);
        }
        if (cur.state() != from) {
            throw new CatalogException("stale transition for " + partition
                    + ": expected " + from + " but was " + cur.state());
        }
        if (!from.canTransitionTo(to)) {
            throw new IllegalTransitionException(partition, from, to);
        }
        partitions.put(partition, new PartitionInfo(partition, cur.bounds(), to));
    }

    public long addReadPin(TableId table, Cutline at) {
        requireTable(table);
        long id = pinSeq.incrementAndGet();
        pins.put(id, new Pin(table, at));
        return id;
    }

    public void releaseReadPin(long pinId) {
        pins.remove(pinId);
    }

    /** Keys handed to {@link #publishCompaction}, for asserting what was cleared. */
    public List<DeltaBatch.Key> clearedDeltaKeys() {
        return List.copyOf(clearedDeltaKeys);
    }

    private void requireTable(TableId table) {
        if (!tables.containsKey(table)) {
            throw new CatalogException("unknown table: " + table);
        }
    }
}
