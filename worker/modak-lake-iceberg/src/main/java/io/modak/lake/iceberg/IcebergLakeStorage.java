package io.modak.lake.iceberg;

import io.modak.common.LakeSnapshotId;
import io.modak.common.RowBatchData.Column;
import io.modak.lake.CommitterInitContext;
import io.modak.lake.LakeCommitResult;
import io.modak.lake.LakeSnapshotReader;
import io.modak.lake.LakeStorage;
import io.modak.lake.LakeTieringFactory;
import io.modak.lake.MaintenanceConfig;
import io.modak.lake.MaintenanceResult;
import io.modak.lake.MergeWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopTables;

/**
 * Iceberg implementation of {@link LakeStorage} (backed by iceberg-java): the tiering
 * factory and the merge writer. Snapshot reader is not implemented — reads go through
 * the Postgres extension (pg_duckdb), not the Java workers.
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

    /** The warehouse-scoped Hadoop configuration (local FS or S3-compatible). */
    public Configuration hadoopConf() {
        return conf;
    }

    /** The table resolver (path-based or REST catalog, per config). */
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
            Set<String> requiredCols, String tierKeyCol, long partitionWidth) {
        return IcebergTableBootstrap.createIfAbsent(
                tables, ref, columns, requiredCols, tierKeyCol, partitionWidth);
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
    public MergeWriter mergeWriter(CommitterInitContext ctx) {
        return new IcebergMergeWriter(tables.load(ctx.lakeTableRef()));
    }

    @Override
    public LakeSnapshotReader snapshotReader() {
        throw new UnsupportedOperationException(
                "Iceberg snapshot reader is not implemented; reads go through the extension");
    }

    @Override
    public void evolveSchema(CommitterInitContext ctx, List<Column> addColumns) {
        IcebergSchemaEvolution.addMissing(tables.load(ctx.lakeTableRef()), addColumns);
    }

    @Override
    public MaintenanceResult maintain(CommitterInitContext ctx, MaintenanceConfig config,
            LakeSnapshotId oldestPinnedSnapshot, Map<String, String> snapshotProps) {
        try {
            return IcebergMaintenance.run(tables.load(ctx.lakeTableRef()), config,
                    oldestPinnedSnapshot.id(), snapshotProps);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "maintenance failed for " + ctx.lakeTableRef(), e);
        }
    }

    @Override
    public LakeCommitResult expireBelow(CommitterInitContext ctx, String tierKeyCol,
            long boundary, Map<String, String> snapshotProps) {
        try {
            return IcebergRetention.expireBelow(
                    tables.load(ctx.lakeTableRef()), tierKeyCol, boundary, snapshotProps);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "retention expiry failed for " + ctx.lakeTableRef(), e);
        }
    }
}
