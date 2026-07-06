package io.tierdb.load;

/** Another load with the same label is mid-transaction. HTTP hosts map this to 409. */
public final class LoadInFlightException extends LoadException {

    public LoadInFlightException(String message) {
        super(message);
    }
}
