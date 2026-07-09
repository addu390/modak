package io.tierdb.load;

import io.tierdb.common.PkCodec;
import io.tierdb.common.TierKeyType;
import io.tierdb.common.mode.ColdSink;
import io.tierdb.common.mode.InsertPlan;
import io.tierdb.common.mode.RouteTarget;
import io.tierdb.connector.seam.SeamState;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Splits a batch against a seam capture by each row's {@link InsertPlan}:
 * heap-bound rows upsert the heap, delta-bound rows go to delta (few) or
 * spool (volume), rows below the retention line reject the batch.
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
            String pk = encodePk(row, state.table().primaryKeyCols(), lineNo);
            if (!seenPks.add(pk)) {
                throw new LoadException("row " + lineNo + " repeats a primary key already in "
                        + "this batch; upsert order within one batch would be undefined");
            }

            long tierKey = tierKey(row, state, lineNo);
            RouteTarget target = tierKey >= state.cutLine().tierKeyHi()
                    ? RouteTarget.HOT : RouteTarget.COLD;
            InsertPlan plan = state.mode().planInsert(target);

            if (plan.checkRetention() && state.cutLine().retentionLine() != null
                    && tierKey < state.cutLine().retentionLine()) {
                throw new LoadException("row " + lineNo + " has tier key " + tierKey
                        + " below the retention line " + state.cutLine().retentionLine()
                        + ", rows this old have been expired from the lake");
            }
            if (plan.cold().orElse(null) == ColdSink.LAKE) {
                throw new LoadException("row " + lineNo + " is cold and the table is direct; "
                        + "bulk load cannot buffer it in the delta, cold rows of a direct "
                        + "table commit straight to the lake");
            }

            if (plan.toHeap()) {
                hot.add(row);
            }
            if (plan.cold().isPresent()) {
                cold.add(row);
            }
        }

        boolean spool = spoolAvailable && cold.size() > spoolThreshold;
        return new Routed(hot,
                spool ? List.of() : cold,
                spool ? cold : List.of());
    }

    static long tierKey(Map<String, Object> row, SeamState state, int lineNo) {
        Object v = row.get(state.table().tierKeyCol());
        if (v == null) {
            throw new LoadException("row " + lineNo + " is missing the tier-key column '"
                    + state.table().tierKeyCol() + "'");
        }
        try {
            return TierKeyType.forType(state.table().tierKeyType()).encode(v);
        } catch (RuntimeException e) {
            throw new LoadException("row " + lineNo + " has an invalid tier-key value '"
                    + v + "' for type " + state.table().tierKeyType(), e);
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
