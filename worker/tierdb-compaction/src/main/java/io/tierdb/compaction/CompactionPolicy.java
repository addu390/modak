package io.tierdb.compaction;

import io.tierdb.common.DeltaBatch;
import io.tierdb.common.TableId;
import java.time.Instant;
import java.util.Optional;

/**
 * Strategy for when and what to fold. Tunes the eager/lazy dial. Eager keeps
 * the delta and cold-read merge cost small but risks small files, lazy makes
 * bigger files but a larger delta. Empty result means nothing to compact.
 */
public interface CompactionPolicy {
    Optional<DeltaBatch> selectForCompaction(TableId table, Instant now);
}
