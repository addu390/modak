package io.tierdb.lake.access;

import java.util.Iterator;

/** An open scan yielding rows aligned to the requested columns. */
public interface RowScan extends Iterator<Object[]>, AutoCloseable {

    @Override
    void close();
}
