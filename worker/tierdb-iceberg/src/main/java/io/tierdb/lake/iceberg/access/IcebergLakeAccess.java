package io.tierdb.lake.iceberg.access;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.access.ColumnConstraint;
import io.tierdb.lake.access.LakeAccess;
import io.tierdb.lake.access.LakeMerge;
import io.tierdb.lake.access.LakeScan;
import io.tierdb.lake.access.RowScan;
import io.tierdb.lake.iceberg.IcebergConfig;
import io.tierdb.lake.iceberg.IcebergPublish;
import io.tierdb.lake.iceberg.IcebergTables;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;

/** The Iceberg implementation of the connector-facing lake port. */
public final class IcebergLakeAccess implements LakeAccess {

    private final Map<String, String> config;
    private final Configuration conf;
    private final IcebergTables tables;

    IcebergLakeAccess(Map<String, String> config) {
        this.config = Map.copyOf(config);
        this.conf = IcebergConfig.hadoopConf(this.config);
        this.tables = IcebergTables.from(this.config, conf);
    }

    @Override
    public Optional<LakeScan> liveScan(String tableRef) {
        Table table = tables.load(tableRef);
        Map<String, String> props = IcebergPublish.props(table);
        if (!props.containsKey("snapshot_id")) {
            return Optional.empty();
        }
        return Optional.of(new LakeScan(props));
    }

    @Override
    public Optional<LakeScan> pinnedScan(Map<String, String> lakeProps) {
        String snapshotId = lakeProps.get("snapshot_id");
        if (snapshotId == null || snapshotId.isBlank()) {
            return Optional.empty();
        }
        return Optional.of(new LakeScan(Map.copyOf(lakeProps)));
    }

    @Override
    public RowScan rows(LakeScan scan, List<String> columns,
            Map<Column, ColumnConstraint> filter) {
        String metadataLocation = scan.props().get("metadata_location");
        if (metadataLocation == null) {
            throw new IllegalStateException("iceberg row scans need 'metadata_location' in the"
                    + " scan props; the cut-line publish did not record one");
        }
        FileIO fileIo = fileIo();
        Table table = new BaseTable(
                new StaticTableOperations(metadataLocation, fileIo), metadataLocation);
        return new IcebergRowScan(table, Long.parseLong(scan.props().get("snapshot_id")),
                fileIo, columns, IcebergPredicates.expression(filter));
    }

    @Override
    public LakeMerge<?> merge(String tableRef, List<String> columns, List<String> pkCols) {
        return new IcebergLakeMerge(tables.load(tableRef), columns, pkCols);
    }

    private FileIO fileIo() {
        ResolvingFileIO io = new ResolvingFileIO();
        io.setConf(conf);
        io.initialize(config);
        return io;
    }
}
