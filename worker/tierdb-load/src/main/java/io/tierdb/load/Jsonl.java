package io.tierdb.load;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JSONL (one object per line) into the row maps a {@link LoadRequest} carries, scalars only. */
public final class Jsonl {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Jsonl() {}

    public static List<Map<String, Object>> parse(BufferedReader reader) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        String line;
        int lineNo = 0;
        while ((line = reader.readLine()) != null) {
            lineNo++;
            if (line.isBlank()) {
                continue;
            }
            JsonNode node = MAPPER.readTree(line);
            if (!node.isObject()) {
                throw new LoadException("line " + lineNo + " is not a json object");
            }
            Map<String, Object> row = new LinkedHashMap<>();
            Iterator<String> fields = node.fieldNames();
            while (fields.hasNext()) {
                String field = fields.next();
                row.put(field, scalar(node.get(field), field, lineNo));
            }
            rows.add(row);
        }
        return rows;
    }

    private static Object scalar(JsonNode v, String field, int lineNo) {
        if (v == null || v.isNull()) {
            return null;
        }
        if (v.isTextual()) {
            return v.textValue();
        }
        if (v.isIntegralNumber()) {
            return v.longValue();
        }
        if (v.isNumber()) {
            return v.doubleValue();
        }
        if (v.isBoolean()) {
            return v.booleanValue();
        }
        throw new LoadException("line " + lineNo + " column '" + field
                + "' is not a scalar: " + v.getNodeType());
    }
}
