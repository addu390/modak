package io.tierdb.lake.access;

import java.io.IOException;
import java.io.Serializable;

/**
 * The distributable half of a {@link LakeMerge}: serializable so engines can
 * ship it to workers, where each participant opens its own writer.
 */
public interface MergeWriterFactory<F> extends Serializable {

    MergeFileWriter<F> newWriter() throws IOException;
}
