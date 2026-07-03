package io.modak.cdc;

import io.modak.cdc.PgOutputMessage.Cell;
import io.modak.cdc.PgOutputMessage.CellKind;
import io.modak.cdc.PgOutputMessage.Column;
import io.modak.cdc.PgOutputMessage.Delete;
import io.modak.cdc.PgOutputMessage.Insert;
import io.modak.cdc.PgOutputMessage.Relation;
import io.modak.cdc.PgOutputMessage.Update;
import io.modak.common.DeltaRowsBatch;
import io.modak.common.PgValues;
import io.modak.common.PkCodec;
import io.modak.common.RowBatchData;
import io.modak.common.TableId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Accumulates decoded changes for one mirrored table and drains them as a
 * {@link DeltaRowsBatch}, newest-wins per PK within the batch. Requires REPLICA
 * IDENTITY FULL (the registrar sets it): deletes need the old image's tier-key,
 * and unchanged-TOAST cells in updates are filled from it.
 */
public final class ChangeBatch {

    private final TableId table;
    private final List<String> pkColumns;
    private final String tierKeyColumn;

    private List<RowBatchData.Column> columns;
    private int[] pkIndexes;
    private int tierKeyIndex = -1;

    private final Map<String, DeltaRowsBatch.Entry> byPk = new LinkedHashMap<>();
    private long versionSeq;

    public ChangeBatch(TableId table, List<String> pkColumns, String tierKeyColumn) {
        this.table = table;
        this.pkColumns = List.copyOf(pkColumns);
        this.tierKeyColumn = tierKeyColumn;
    }

    /**
     * Classifies the incoming Relation against the adopted layout. Returns
     * columns to add to the lake (all on a stream's first Relation, appended
     * ones on ADD COLUMN, none when unchanged). Destructive changes throw
     * {@link SchemaDivergedException}.
     */
    public List<RowBatchData.Column> classify(Relation relation) {
        if (columns == null) {
            List<RowBatchData.Column> all = new ArrayList<>(relation.columns().size());
            for (Column c : relation.columns()) {
                all.add(columnOf(c));
            }
            return all;
        }
        String name = relation.schemaName() + "." + relation.tableName();
        List<Column> incoming = relation.columns();
        if (incoming.size() < columns.size()) {
            List<String> incomingNames = incoming.stream().map(Column::name).toList();
            List<String> dropped = columns.stream()
                    .map(RowBatchData.Column::name)
                    .filter(n -> !incomingNames.contains(n))
                    .toList();
            throw new SchemaDivergedException("mirrored table " + name
                    + " lost column(s) " + dropped + ", re-register the table to re-sync");
        }
        for (int i = 0; i < columns.size(); i++) {
            String was = columns.get(i).name();
            String now = incoming.get(i).name();
            if (!was.equals(now)) {
                throw new SchemaDivergedException("mirrored table " + name
                        + " column " + (i + 1) + " changed from '" + was + "' to '" + now
                        + "' (rename / drop / reorder), re-register the table to re-sync");
            }
            RowBatchData.ColumnType wasType = columns.get(i).type();
            RowBatchData.ColumnType nowType = columnOf(incoming.get(i)).type();
            if (wasType != nowType) {
                throw new SchemaDivergedException("mirrored table " + name
                        + " column '" + was + "' changed type " + wasType + " -> " + nowType
                        + ", re-register the table to re-sync");
            }
        }
        List<RowBatchData.Column> added = new ArrayList<>();
        for (int i = columns.size(); i < incoming.size(); i++) {
            added.add(columnOf(incoming.get(i)));
        }
        return added;
    }

    /** Column layout for subsequent changes. pgoutput re-announces it per stream and on DDL. */
    public void onRelation(Relation relation) {
        List<RowBatchData.Column> mapped = new ArrayList<>(relation.columns().size());
        int[] pk = new int[pkColumns.size()];
        Arrays.fill(pk, -1);
        int tier = -1;
        for (int i = 0; i < relation.columns().size(); i++) {
            Column c = relation.columns().get(i);
            mapped.add(columnOf(c));
            int pkPos = pkColumns.indexOf(c.name());
            if (pkPos >= 0) {
                pk[pkPos] = i;
            }
            if (c.name().equals(tierKeyColumn)) {
                tier = i;
            }
        }
        for (int i = 0; i < pk.length; i++) {
            if (pk[i] < 0) {
                throw new CdcException("relation " + relation.schemaName() + "."
                        + relation.tableName() + " is missing pk column '" + pkColumns.get(i) + "'");
            }
        }
        if (tier < 0) {
            throw new CdcException("relation " + relation.schemaName() + "." + relation.tableName()
                    + " is missing tier-key '" + tierKeyColumn + "'");
        }
        this.columns = List.copyOf(mapped);
        this.pkIndexes = pk;
        this.tierKeyIndex = tier;
    }

    public void onInsert(Insert insert) {
        upsert(insert.newRow(), null);
    }

    public void onUpdate(Update update) {
        upsert(update.newRow(), update.oldRow());
    }

    public void onDelete(Delete delete) {
        requireRelation();
        Object[] old = materialize(delete.oldRow(), null);
        String pk = pkOf(old);
        long tierKey = tierKeyOf(old);
        // The old image stays on the tombstone: the equality delete needs typed pk values.
        byPk.put(pk, new DeltaRowsBatch.Entry(
                pk, true, tierKey, lakeTierKey(pk, tierKey, tierKey), nextVersion(), old));
    }

    private void upsert(List<Cell> newRow, List<Cell> oldRow) {
        requireRelation();
        Object[] row = materialize(newRow, oldRow);
        String pk = pkOf(row);
        long tierKey = tierKeyOf(row);
        long eventOldTier = oldRow == null ? tierKey
                : tierKeyOf(materialize(oldRow, null));
        byPk.put(pk, new DeltaRowsBatch.Entry(
                pk, false, tierKey, lakeTierKey(pk, tierKey, eventOldTier), nextVersion(), row));
    }

    /**
     * Where the lake still holds this pk's image, or null when that is the
     * entry's own tier. An UPDATE that changes the tier key must delete the old
     * partition's image, and earlier changes in the batch already track it.
     */
    private Long lakeTierKey(String pk, long tierKey, long eventOldTier) {
        DeltaRowsBatch.Entry prior = byPk.get(pk);
        long lake = prior != null ? prior.lakeTierKey() : eventOldTier;
        return lake == tierKey ? null : lake;
    }

    private String pkOf(Object[] row) {
        List<String> parts = new ArrayList<>(pkIndexes.length);
        for (int idx : pkIndexes) {
            Object v = row[idx];
            if (v == null) {
                throw new CdcException("pk column '" + columns.get(idx).name() + "' is NULL");
            }
            parts.add(String.valueOf(v));
        }
        return PkCodec.encode(parts);
    }

    public boolean isEmpty() {
        return byPk.isEmpty();
    }

    public int size() {
        return byPk.size();
    }

    /** The collapsed batch, clearing the accumulator for the next window. */
    public DeltaRowsBatch drain() {
        requireRelation();
        DeltaRowsBatch batch = new DeltaRowsBatch(
                table, pkColumns, columns, List.copyOf(byPk.values()));
        byPk.clear();
        return batch;
    }

    private Object[] materialize(List<Cell> cells, List<Cell> oldRow) {
        if (cells.size() != columns.size()) {
            throw new CdcException("tuple has " + cells.size() + " cells but the relation has "
                    + columns.size() + " columns, missed a Relation message?");
        }
        Object[] row = new Object[cells.size()];
        for (int i = 0; i < cells.size(); i++) {
            Cell cell = cells.get(i);
            if (cell.kind() == CellKind.UNCHANGED_TOAST) {
                if (oldRow == null || oldRow.get(i).kind() == CellKind.UNCHANGED_TOAST) {
                    throw new CdcException("unchanged-TOAST cell for '" + columns.get(i).name()
                            + "' with no old image, is REPLICA IDENTITY FULL set?");
                }
                cell = oldRow.get(i);
            }
            row[i] = cell.kind() == CellKind.NULL ? null
                    : PgValues.parseText(cell.text(), columns.get(i).type());
        }
        return row;
    }

    private long tierKeyOf(Object[] row) {
        Object v = row[tierKeyIndex];
        if (v instanceof Long l) {
            return l;
        }
        throw new CdcException("tier-key column '" + tierKeyColumn + "' must decode to a long, got "
                + (v == null ? "NULL" : v.getClass().getSimpleName()));
    }

    private long nextVersion() {
        return ++versionSeq;
    }

    private void requireRelation() {
        if (columns == null) {
            throw new CdcException("no Relation message seen yet for " + table);
        }
    }

    // Postgres type OIDs (pg_type.dat) -> the portable column vocabulary.
    private static RowBatchData.Column columnOf(Column c) {
        return switch (c.typeOid()) {
            case 16 -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.BOOLEAN);
            case 20, 21, 23, 26 -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.LONG);
            case 700, 701 -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.DOUBLE);
            case 1700 -> numericColumn(c);
            case 1114, 1184 -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.TIMESTAMP);
            case 1082 -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.DATE);
            case 2950 -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.UUID);
            case 17 -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.BINARY);
            default -> new RowBatchData.Column(c.name(), RowBatchData.ColumnType.TEXT);
        };
    }

    // numeric typmod packs (precision << 16 | scale) + 4; -1 = unconstrained -> double.
    private static RowBatchData.Column numericColumn(Column c) {
        if (c.typeMod() < 4) {
            return new RowBatchData.Column(c.name(), RowBatchData.ColumnType.DOUBLE);
        }
        int packed = c.typeMod() - 4;
        return new RowBatchData.Column(c.name(), RowBatchData.ColumnType.DECIMAL,
                (packed >> 16) & 0xFFFF, packed & 0xFFFF);
    }
}
