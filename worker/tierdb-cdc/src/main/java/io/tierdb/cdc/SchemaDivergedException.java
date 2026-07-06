package io.tierdb.cdc;

/**
 * A mirrored table's schema changed destructively (drop / rename / type change /
 * reorder). Never retried: the pump stops and the operator re-registers the
 * table to re-sync.
 */
public final class SchemaDivergedException extends CdcException {

    public SchemaDivergedException(String message) {
        super(message);
    }
}
