package io.modak.catalog;

/** Lifecycle of a Stream Load label in {@code modak.load_labels}. */
public enum LoadState {
    /** Cold parquet written and registered, waiting for the worker to adopt it. */
    STAGED,
    /** Fully applied: heap/delta effects visible, staged files committed to the lake. */
    COMMITTED,
    /** Permanently rejected, replays get the same answer. */
    FAILED;

    /** The catalog's lowercase representation. */
    public String sql() {
        return name().toLowerCase();
    }

    public static LoadState fromSql(String state) {
        return valueOf(state.toUpperCase());
    }
}
