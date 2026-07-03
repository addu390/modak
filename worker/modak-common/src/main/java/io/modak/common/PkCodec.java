package io.modak.common;

import java.util.List;

/**
 * Canonical text encoding of a (possibly composite) primary key, matching the
 * Rust side ({@code modak_core::sqlgen::encode_pk}) byte for byte. Single-column
 * keys stay the raw text value, multi-column keys escape {@code \} and the unit
 * separator (0x1F) in each part with {@code \}, then join on 0x1F.
 */
public final class PkCodec {

    private static final char SEP = '\u001f';

    private PkCodec() {}

    public static String encode(List<String> values) {
        if (values.size() == 1) {
            return values.get(0);
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(SEP);
            }
            sb.append(values.get(i)
                    .replace("\\", "\\\\")
                    .replace(String.valueOf(SEP), "\\" + SEP));
        }
        return sb.toString();
    }
}
