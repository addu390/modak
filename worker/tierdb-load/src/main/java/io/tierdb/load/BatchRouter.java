package io.tierdb.load;

import io.tierdb.common.PkCodec;
import io.tierdb.common.TierKeyType;
import io.tierdb.connector.seam.SeamState;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Splits a batch against a seam capture: {@code >= T} is hot, {@code [R, T)}
 * goes to delta (few) or spool (volume), below {@code R} rejects the batch.
 */
final class BatchRouter {

    record Routed(
            List<Map<String, Object>> hot,
            List<Map<String, Object>> coldDelta,
            List<Map<String, Object>> coldSpool) {

        boolean spools() {
            return !coldSpool.isEmpty();
        }
    }

    private BatchRouter() {}

    static Routed route(List<Map<String, Object>> rows, SeamState state,
            boolean spoolAvailable, int spoolThreshold) {
        List<Map<String, Object>> hot = new ArrayList<>();
        List<Map<String, Object>> cold = new ArrayList<>();
        Set<String> seenPks = new HashSet<>();

        int lineNo = 0;
        for (Map<String, Object> row : rows) {
            lineNo++;
            String pk = encodePk(row, state.primaryKeyCols(), lineNo);
            if (!seenPks.add(pk)) {
                throw new LoadException("row " + lineNo + " repeats a primary key already in "
                        + "this batch; upsert order within one batch would be undefined");
            }
            long tierKey = tierKey(row, state, lineNo);
            if (state.heapIsComplete() || tierKey >= state.tierKeyHi()) {
                hot.add(row);
                continue;
            }
            if (state.retentionLine() != null && tierKey < state.retentionLine()) {
                throw new LoadException("row " + lineNo + " has tier key " + tierKey
                        + " below the retention line " + state.retentionLine()
                        + ", rows this old have been expired from the lake");
            }
            cold.add(row);
        }

        boolean spool = spoolAvailable && cold.size() > spoolThreshold;
        return new Routed(hot,
                spool ? List.of() : cold,
                spool ? cold : List.of());
    }

    static long tierKey(Map<String, Object> row, SeamState state, int lineNo) {
        Object v = row.get(state.tierKeyCol());
        if (v == null) {
            throw new LoadException("row " + lineNo + " is missing the tier-key column '"
                    + state.tierKeyCol() + "'");
        }
        try {
            return TierKeyType.forType(state.tierKeyType()).encode(v);
        } catch (RuntimeException e) {
            throw new LoadException("row " + lineNo + " has an invalid tier-key value '"
                    + v + "' for type " + state.tierKeyType(), e);
        }
    }

    static String encodePk(Map<String, Object> row, List<String> pkCols, int lineNo) {
        List<String> parts = new ArrayList<>(pkCols.size());
        for (String col : pkCols) {
            Object v = row.get(col);
            if (v == null) {
                throw new LoadException("row " + lineNo + " is missing primary-key column '"
                        + col + "'");
            }
            parts.add(String.valueOf(v));
        }
        return PkCodec.encode(parts);
    }
}
