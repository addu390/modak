package io.modak.tiering;

/**
 * Post-commit housekeeping (partition DROP) failed. The lake commit and
 * cut-line advance already happened and are never unwound. Not data loss,
 * the DROP retries next cycle.
 */
public class ReclaimException extends TieringException {

    public ReclaimException(String message, Throwable cause) {
        super(message, cause);
    }
}
