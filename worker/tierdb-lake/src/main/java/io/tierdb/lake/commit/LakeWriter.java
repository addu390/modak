package io.tierdb.lake.commit;

import io.tierdb.common.PartitionData;
import java.io.Closeable;
import java.io.IOException;

/**
 * Writes a partition's rows to the cold store as data files. {@code WriteResult}
 * is the opaque per-format handle the {@link LakeCommitter} aggregates into one
 * snapshot.
 */
public interface LakeWriter<WriteResult> extends Closeable {
    void write(PartitionData data) throws IOException;

    WriteResult complete() throws IOException;
}
