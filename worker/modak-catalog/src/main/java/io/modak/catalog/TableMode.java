package io.modak.catalog;

/**
 * How a registered table keeps its lake copy. One frame unifies the modes.
 * The lake holds data below a frontier F, Postgres above a retention line R,
 * and reads split at a seam T with R &lt;= T &lt;= F.
 */
public enum TableMode {
    /**
     * Data physically moves. Recent rows live in Postgres, rows older than the
     * cut-line T live only in the lake (R = T = F). Partitioned tables only.
     */
    TIERED,
    /**
     * CDC keeps a trailing full copy in the lake. Postgres keeps everything
     * (R = -inf) unless {@code heap_retention_lag} re-introduces partition drops
     * below R, giving recent-only in Postgres and the entirety in the lake.
     */
    MIRRORED;

    /** The catalog's lowercase representation in {@code modak.tables.mode}. */
    public String sql() {
        return name().toLowerCase();
    }

    public static TableMode fromSql(String mode) {
        return valueOf(mode.toUpperCase());
    }
}
