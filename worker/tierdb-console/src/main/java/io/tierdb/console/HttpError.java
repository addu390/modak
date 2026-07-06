package io.tierdb.console;

/** An API failure carrying the HTTP status and the JSON body to send. */
final class HttpError extends Exception {

    private final int status;
    private final String bodyJson;

    HttpError(int status, String message) {
        super(message);
        this.status = status;
        this.bodyJson = "{\"error\":" + Json.str(message) + "}";
    }

    private HttpError(int status, String bodyJson, boolean raw) {
        super(bodyJson);
        this.status = status;
        this.bodyJson = bodyJson;
    }

    static HttpError raw(int status, String bodyJson) {
        return new HttpError(status, bodyJson, true);
    }

    int status() {
        return status;
    }

    String bodyJson() {
        return bodyJson;
    }
}
