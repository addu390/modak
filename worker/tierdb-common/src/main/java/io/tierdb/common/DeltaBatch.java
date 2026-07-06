package io.tierdb.common;

import java.util.List;

/**
 * A batch of {@code tierdb.delta} entries selected for compaction. The core
 * reasons about batch identity ({@link #keys()}) to clear exactly the folded
 * rows. A row re-corrected mid-compaction has a newer version and survives the clear.
 */
public interface DeltaBatch {
    TableId table();

    int size();

    List<Key> keys();

    /** Identity of one folded delta row: its PK and the version that was folded. */
    record Key(String pk, long version) {}
}
