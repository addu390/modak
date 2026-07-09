package io.tierdb.lake.access;

import java.io.Closeable;
import java.io.IOException;

/**
 * Writes one participant's share of a merge as files. {@code F} is the opaque
 * per-format handle that {@link LakeMerge#commit} aggregates into one snapshot.
 * Rows align to the columns the merge was opened with; deletes need only the
 * PK columns and the tier key populated.
 */
public interface MergeFileWriter<F> extends Closeable {

    void upsert(Object[] row) throws IOException;

    void delete(Object[] row) throws IOException;

    F complete() throws IOException;
}
