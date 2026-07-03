package io.modak.catalog;

/** Base for catalog operation failures (unchecked, wraps SQL/consistency errors). */
public class CatalogException extends RuntimeException {
    public CatalogException(String message) {
        super(message);
    }

    public CatalogException(String message, Throwable cause) {
        super(message, cause);
    }
}
