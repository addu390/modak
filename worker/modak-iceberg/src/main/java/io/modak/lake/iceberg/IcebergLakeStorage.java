package io.modak.lake.iceberg;

import io.modak.common.RowBatchData.Column;
import io.modak.lake.ColdTableSpec;
import io.modak.lake.commit.CommitterInitContext;
import io.modak.lake.LakePartition;
import io.modak.lake.LakeSnapshotReader;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTable;
import io.modak.lake.commit.LakeTieringFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;

/**
 * Iceberg implementation of {@link LakeStorage} (backed by iceberg-java), the
 * tiering factory and the merge writer. Snapshot reader is not implemented,
 * reads go through the Postgres extension (pg_duckdb), not the Java workers.
 */
public final class IcebergLakeStorage implements LakeStorage {

    private final Map<String, String> config;
    private final Configuration conf;
    private final IcebergTables tables;

    public IcebergLakeStorage(Map<String, String> config) {
        this.config = Map.copyOf(config);
        this.conf = IcebergConfig.hadoopConf(this.config);
        this.tables = IcebergTables.from(this.config, this.conf);
    }

    Map<String, String> config() {
        return config;
    }

    public Configuration hadoopConf() {
        return conf;
    }

    public IcebergTables tables() {
        return tables;
    }

    @Override
    public String tableRef(String schema, String table) {
        if (tables.viaCatalog()) {
            return config.getOrDefault("catalog.namespace", "modak")
                    + "." + schema + "_" + table;
        }
        String warehouse = config.get("warehouse");
        if (warehouse == null || warehouse.isBlank()) {
            throw new IllegalStateException("lake config has no 'warehouse'");
        }
        return warehouse.replaceAll("/+$", "") + "/" + schema + "." + table;
    }

    @Override
    public String createTableIfAbsent(String ref, List<Column> columns,
            Set<String> requiredCols, String tierKeyCol, LakePartition partition) {
        return IcebergTableBootstrap.createIfAbsent(
                tables, ref, columns, requiredCols, tierKeyCol, partition);
    }

    @Override
    public void dropTable(String ref) {
        tables.drop(ref);
    }

    @Override
    public LakeTieringFactory<?, ?> tieringFactory() {
        return new IcebergTieringFactory(tables);
    }

    @Override
    public LakeSnapshotReader snapshotReader() {
        throw new UnsupportedOperationException(
                "Iceberg snapshot reader is not implemented; reads go through the extension");
    }

    @Override
    public LakeTable table(CommitterInitContext ctx, ColdTableSpec spec) {
        return new IcebergLakeTable(tables.load(ctx.lakeTableRef()), spec);
    }
}
