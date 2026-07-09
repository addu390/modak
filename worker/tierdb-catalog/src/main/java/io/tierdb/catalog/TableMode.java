package io.tierdb.catalog;

/**
 * How a registered table keeps its lake copy. One frame unifies the modes.
 * The lake holds data below a frontier F, Postgres above a retention line R,
 * and reads split at a seam T with R &lt;= T &lt;= F.
 */
public enum TableMode {
    TIERED,
    DIRECT,
    MIRRORED;

    /** Tiered and direct split rows at the cut-line; they differ only in the cold sink. */
    public boolean tierSplitting() {
        return this != MIRRORED;
    }

    public String sql() {
        return name().toLowerCase();
    }

    public static TableMode fromSql(String mode) {
        return valueOf(mode.toUpperCase());
    }
}
