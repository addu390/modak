package io.tierdb.common;

import io.tierdb.common.RowBatchData.Column;
import java.util.List;

/**
 * Fully-materialized delta entries with typed row values. Upserts carry the
 * full row image positional per {@link #columns()}. Tombstones carry at least
 * the pk fields, whose typed values the equality delete needs.
 */
public record DeltaRowsBatch(
        TableId table,
        List<String> pkColumns,
        List<Column> columns,
        List<Entry> entries) implements DeltaBatch {

    /**
     * One delta entry. {@code pk} is the canonical {@link PkCodec} text.
     * {@code oldTierKey} is set when the row moved tiers and the lake still
     * holds its image in the old partition. Most entries never move.
     */
    public record Entry(String pk, boolean tombstone, long tierKey, Long oldTierKey,
            long version, Object[] row) {

        public Entry(String pk, boolean tombstone, long tierKey, long version, Object[] row) {
            this(pk, tombstone, tierKey, null, version, row);
        }

        public long lakeTierKey() {
            return oldTierKey != null ? oldTierKey : tierKey;
        }
    }

    public DeltaRowsBatch {
        pkColumns = List.copyOf(pkColumns);
        columns = List.copyOf(columns);
        entries = List.copyOf(entries);
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public List<Key> keys() {
        return entries.stream().map(e -> new Key(e.pk(), e.version())).toList();
    }
}
