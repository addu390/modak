package io.tierdb.lake.access;

import java.io.IOException;
import java.util.List;

/**
 * One merge (upserts + deletes) against a lake table, mirroring the
 * {@link io.tierdb.lake.commit.LakeWriter}/{@link io.tierdb.lake.commit.LakeCommitter}
 * split: workers write files through {@link #writerFactory()}, the coordinator
 * commits every result as one atomic snapshot or aborts, cleaning up the files.
 */
public interface LakeMerge<F> {

    MergeWriterFactory<F> writerFactory();

    void commit(List<F> results) throws IOException;

    void abort(List<F> results) throws IOException;
}
