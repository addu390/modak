package io.modak.common;

/**
 * Every kind of lake operation Modak runs. One value drives both the
 * {@code modak.tiering_log} journal and the snapshot stamps.
 */
public enum OpKind {
    TIERING("tiering"),
    COMPACTION("compaction"),
    MIRROR("mirror"),
    MAINTENANCE("maintenance"),
    RETENTION("retention"),
    INGEST("ingest"),
    LOAD("load");

    private final String sql;

    OpKind(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }

    /** The {@code commit-user} snapshot stamp maintenance tools filter on. */
    public String commitUser() {
        return "__modak_" + sql;
    }

    public static OpKind fromSql(String sql) {
        for (OpKind kind : values()) {
            if (kind.sql.equals(sql)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown op kind: " + sql);
    }
}
