package io.tierdb.load;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.tierdb.catalog.LoadState;
import java.util.ArrayList;
import java.util.List;

/**
 * What one labeled load did: where its rows went. {@code replay} means the
 * label had already finished and this is its recorded result.
 */
public record LoadResult(
        String label,
        LoadState state,
        long hotRows,
        long deltaRows,
        long spooledRows,
        List<String> stagedFiles,
        boolean replay) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public String toJson() {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("label", label);
        node.put("state", state.sql());
        node.put("hot_rows", hotRows);
        node.put("delta_rows", deltaRows);
        node.put("spooled_rows", spooledRows);
        ArrayNode files = node.putArray("staged_files");
        stagedFiles.forEach(files::add);
        node.put("replay", replay);
        return node.toString();
    }

    public static LoadResult fromJson(String json, boolean replay) {
        try {
            JsonNode node = MAPPER.readTree(json);
            List<String> files = new ArrayList<>();
            node.path("staged_files").forEach(f -> files.add(f.asText()));
            return new LoadResult(
                    node.path("label").asText(),
                    LoadState.fromSql(node.path("state").asText()),
                    node.path("hot_rows").asLong(),
                    node.path("delta_rows").asLong(),
                    node.path("spooled_rows").asLong(),
                    files,
                    replay);
        } catch (Exception e) {
            throw new LoadException("unreadable stored load result: " + json, e);
        }
    }
}
