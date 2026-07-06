package io.tierdb.tiering;

import io.tierdb.catalog.PartitionInfo;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.PartitionData;
import io.tierdb.common.PartitionId;
import io.tierdb.common.RowBatchData;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Hot side for worker tests, rows per partition. Drops are recorded, not executed. */
final class FakeHotSource implements HotSource {

    static final List<Column> COLUMNS = List.of(
            new Column("id", ColumnType.LONG),
            new Column("event_time", ColumnType.LONG),
            new Column("val", ColumnType.TEXT));

    final Map<PartitionId, List<Object[]>> rowsByPartition = new HashMap<>();
    final List<PartitionId> dropped = new ArrayList<>();
    final List<PartitionId> mirrored = new ArrayList<>();
    boolean failOnDrop;

    void seed(PartitionId partition, Object[]... rows) {
        rowsByPartition.put(partition, List.of(rows));
    }

    @Override
    public PartitionData read(RegisteredTable table, PartitionInfo partition) {
        return new RowBatchData(partition.id(), partition.bounds(), COLUMNS,
                rowsByPartition.getOrDefault(partition.id(), List.of()));
    }

    @Override
    public void attachColdMirror(RegisteredTable table, PartitionId partition) {
        if (!mirrored.contains(partition)) {
            mirrored.add(partition);
        }
    }

    @Override
    public void dropPartition(RegisteredTable table, PartitionId partition) {
        if (failOnDrop) {
            throw new IllegalStateException("injected DROP failure for " + partition);
        }
        dropped.add(partition);
    }
}
