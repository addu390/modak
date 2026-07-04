package io.modak.load;

import java.util.List;
import java.util.Map;

/** One labeled micro-batch. The label is the client's idempotency handle. */
public record LoadRequest(String label, List<Map<String, Object>> rows) {

    public LoadRequest {
        if (label == null || label.isBlank()) {
            throw new LoadException("a load needs a non-blank label");
        }
        rows = List.copyOf(rows);
    }
}
