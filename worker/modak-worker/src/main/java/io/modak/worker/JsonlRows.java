package io.modak.worker;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modak.common.RowBatchData.Column;
import io.modak.lake.TierKeyWindow;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Streams a JSONL file (one object per line, keys matching column names) as
 * rows in the given column order, validating each row's tier key against the
 * window as it goes. {@link #validate()} is a full dry pass, so callers can
 * reject bad input before anything is written.
 */
final class JsonlRows implements Iterable<Object[]> {

    private final Path file;
    private final List<String> columnNames;
    private final Map<String, Integer> indexOf;
    private final String tierKeyCol;
    private final TierKeyWindow window;
    private final ObjectMapper mapper = new ObjectMapper();

    JsonlRows(Path file, List<Column> columns, String tierKeyCol, TierKeyWindow window) {
        this.file = file;
        this.columnNames = columns.stream().map(Column::name).toList();
        this.indexOf = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            indexOf.put(columnNames.get(i), i);
        }
        this.tierKeyCol = tierKeyCol;
        this.window = window;
    }

    List<String> columnNames() {
        return columnNames;
    }

    void validate() {
        for (Object[] ignored : this) {
            // parsing and window checks happen per line
        }
    }

    boolean isEmpty() {
        return !iterator().hasNext();
    }

    @Override
    public Iterator<Object[]> iterator() {
        BufferedReader reader;
        try {
            reader = Files.newBufferedReader(file);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
        return new Iterator<>() {
            private Object[] next;
            private int lineNo;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    next = readRow();
                }
                return next != null;
            }

            @Override
            public Object[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Object[] row = next;
                next = null;
                return row;
            }

            private Object[] readRow() {
                try {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lineNo++;
                        if (!line.isBlank()) {
                            return parseRow(line, lineNo);
                        }
                    }
                    reader.close();
                    return null;
                } catch (IOException e) {
                    throw new UncheckedIOException("cannot read " + file, e);
                }
            }
        };
    }

    private Object[] parseRow(String line, int lineNo) throws IOException {
        JsonNode node = mapper.readTree(line);
        if (!node.isObject()) {
            throw new IllegalArgumentException(file + " line " + lineNo
                    + " is not a json object");
        }
        Object[] row = new Object[columnNames.size()];
        Iterator<String> fields = node.fieldNames();
        while (fields.hasNext()) {
            String field = fields.next();
            Integer idx = indexOf.get(field);
            if (idx == null) {
                throw new IllegalArgumentException(file + " line " + lineNo
                        + " has unknown column '" + field + "'");
            }
            row[idx] = scalar(node.get(field), field, lineNo);
        }
        if (!(row[indexOf.get(tierKeyCol)] instanceof Number tierKey)) {
            throw new IllegalArgumentException(file + " line " + lineNo
                    + " is missing tier-key column '" + tierKeyCol + "'");
        }
        long k = tierKey.longValue();
        if (!window.contains(k)) {
            throw new IllegalArgumentException(file + " line " + lineNo + " has tier key "
                    + k + " outside the ingest window " + window);
        }
        return row;
    }

    private Object scalar(JsonNode v, String field, int lineNo) {
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
        throw new IllegalArgumentException(file + " line " + lineNo + " column '" + field
                + "' is not a scalar: " + v.getNodeType());
    }
}
