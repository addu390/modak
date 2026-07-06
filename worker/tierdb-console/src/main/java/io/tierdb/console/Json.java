package io.tierdb.console;

/** Minimal JSON building helpers for the console API (no dependencies). */
final class Json {

    private Json() {}

    static String str(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }

    static String num(Long v) {
        return v == null ? "null" : Long.toString(v);
    }

    static String num(Double v) {
        if (v == null || v.isNaN() || v.isInfinite()) {
            return "null";
        }
        return v == Math.floor(v) ? Long.toString((long) (double) v) : Double.toString(v);
    }

    static String bool(boolean v) {
        return v ? "true" : "false";
    }

    static String raw(String json) {
        return json == null || json.isBlank() ? "null" : json;
    }
}
