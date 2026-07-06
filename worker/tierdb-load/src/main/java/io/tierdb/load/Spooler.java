package io.tierdb.load;

import io.tierdb.common.TableId;
import io.tierdb.common.TierKeyType;
import io.tierdb.connector.seam.SeamState;
import io.tierdb.lake.ColdTableSpec;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.LakeStorage;
import io.tierdb.lake.LakeTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Cold volume as staged parquet. Commits nothing, the files stay invisible
 * until the worker's adoption pass commits them.
 */
final class Spooler {

    private Spooler() {}

    static StagedFiles spool(LakeStorage lake, SeamState state, List<String> columns,
            List<Map<String, Object>> rows) {
        LakeTable table = lake.table(
                new CommitterInitContext(new TableId(state.tableId()), state.lakeTableRef()),
                new ColdTableSpec(state.primaryKeyCols(), state.tierKeyCol()));

        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        List<Object[]> tuples = new ArrayList<>(rows.size());
        int tierKeyIdx = columns.indexOf(state.tierKeyCol());
        TierKeyType tierKeyType = TierKeyType.forType(state.tierKeyType());
        for (Map<String, Object> row : rows) {
            Object[] tuple = new Object[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                tuple[i] = row.get(columns.get(i));
            }
            long tierKey = tierKeyType.encode(tuple[tierKeyIdx]);
            min = Math.min(min, tierKey);
            max = Math.max(max, tierKey);
            tuples.add(tuple);
        }
        return new StagedFiles(table.stageRows(columns, tuples), min, max);
    }
}
