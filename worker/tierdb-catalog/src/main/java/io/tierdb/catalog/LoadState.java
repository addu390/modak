package io.tierdb.catalog;

/** Lifecycle of a Stream Load label in {@code tierdb.load_labels}. */
public enum LoadState {
    STAGED,
    COMMITTED,
    FAILED;

    public String sql() {
        return name().toLowerCase();
    }

    public static LoadState fromSql(String state) {
        return valueOf(state.toUpperCase());
    }
}
