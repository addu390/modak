package io.tierdb.tiering;

/** A tiering step failed, the op stays resumable via {@code tierdb.op_log}. */
public class TieringException extends RuntimeException {
    public TieringException(String message) {
        super(message);
    }

    public TieringException(String message, Throwable cause) {
        super(message, cause);
    }
}
