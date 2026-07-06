package io.tierdb.lake.commit;

import java.io.IOException;

/**
 * Abstract Factory: produces the matched {@link LakeWriter} + {@link LakeCommitter}
 * for one lake format.
 */
public interface LakeTieringFactory<WriteResult, Committable> {
    LakeWriter<WriteResult> createWriter(WriterInitContext ctx) throws IOException;

    LakeCommitter<WriteResult, Committable> createCommitter(CommitterInitContext ctx) throws IOException;
}
