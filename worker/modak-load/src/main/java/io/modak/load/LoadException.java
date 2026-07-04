package io.modak.load;

/** A load rejected before anything committed, the same label may retry. */
public class LoadException extends RuntimeException {

    public LoadException(String message) {
        super(message);
    }

    public LoadException(String message, Throwable cause) {
        super(message, cause);
    }
}
