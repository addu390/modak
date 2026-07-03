package io.modak.tiering;

/** A tiering step failed, the op stays resumable via {@code modak.tiering_log}. */
public class TieringException extends RuntimeException {
    public TieringException(String message) {
        super(message);
    }

    public TieringException(String message, Throwable cause) {
        super(message, cause);
    }
}
