package io.modak.load;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The staged-parquet manifest in {@code modak.load_labels.staged_files}:
 * file paths plus the tier-key range they cover. Owns the JSON round trip.
 */
public record StagedFiles(List<String> files, long minTierKey, long maxTierKey) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String toJson() {
        var node = MAPPER.createObjectNode();
        var arr = node.putArray("files");
        files.forEach(arr::add);
        node.put("lo", minTierKey);
        node.put("hi", maxTierKey);
        return node.toString();
    }

    /** Empty for a load that never spooled ({@code null} column). */
    public static Optional<StagedFiles> fromJson(String json) {
        if (json == null) {
            return Optional.empty();
        }
        try {
            JsonNode node = MAPPER.readTree(json);
            List<String> files = new ArrayList<>();
            node.path("files").forEach(f -> files.add(f.asText()));
            return Optional.of(new StagedFiles(List.copyOf(files),
                    node.path("lo").asLong(Long.MAX_VALUE),
                    node.path("hi").asLong(Long.MIN_VALUE)));
        } catch (Exception e) {
            throw new IllegalStateException("unreadable staged_files: " + json, e);
        }
    }
}
