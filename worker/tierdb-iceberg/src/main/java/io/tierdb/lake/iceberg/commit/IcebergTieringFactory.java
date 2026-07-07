package io.tierdb.lake.iceberg.commit;

import io.tierdb.lake.iceberg.IcebergTables;
import io.tierdb.lake.commit.CommitterInitContext;
import io.tierdb.lake.commit.LakeCommitter;
import io.tierdb.lake.commit.LakeTieringFactory;
import io.tierdb.lake.commit.LakeWriter;
import io.tierdb.lake.commit.WriterInitContext;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;

/**
 * Iceberg tiering: Parquet {@link LakeWriter} + snapshot {@link LakeCommitter},
 * with table resolution through {@link IcebergTables}.
 */
public final class IcebergTieringFactory
        implements LakeTieringFactory<IcebergWriteResult, IcebergCommittable> {

    private final IcebergTables tables;

    public IcebergTieringFactory() {
        this(new Configuration());
    }

    public IcebergTieringFactory(Configuration conf) {
        this(IcebergTables.from(Map.of(), conf));
    }

    public IcebergTieringFactory(IcebergTables tables) {
        this.tables = tables;
    }

    @Override
    public LakeWriter<IcebergWriteResult> createWriter(WriterInitContext ctx) throws IOException {
        return new IcebergLakeWriter(loadTable(ctx.lakeTableRef()), ctx);
    }

    @Override
    public LakeCommitter<IcebergWriteResult, IcebergCommittable> createCommitter(
            CommitterInitContext ctx) throws IOException {
        return new IcebergLakeCommitter(loadTable(ctx.lakeTableRef()));
    }

    private Table loadTable(String ref) throws IOException {
        try {
            return tables.load(ref);
        } catch (RuntimeException e) {
            throw new IOException("failed to load Iceberg table " + ref, e);
        }
    }
}
