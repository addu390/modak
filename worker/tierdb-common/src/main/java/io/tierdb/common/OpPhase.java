package io.tierdb.common;

/**
 * The last phase an op durably reached in {@code tierdb.op_log}. Abandoned
 * means the worker crashed before the lake commit and the op is safe to redo.
 */
public enum OpPhase {
    FLUSHING("flushing"),
    COMMITTED("committed"),
    ADVANCED("advanced"),
    ABANDONED("abandoned");

    private final String sql;

    OpPhase(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return sql;
    }

    public boolean isTerminal() {
        return this == ADVANCED || this == ABANDONED;
    }

    public static OpPhase fromSql(String sql) {
        for (OpPhase phase : values()) {
            if (phase.sql.equals(sql)) {
                return phase;
            }
        }
        throw new IllegalArgumentException("unknown op phase: " + sql);
    }
}
