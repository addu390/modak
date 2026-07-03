package io.modak.worker;

import java.util.ArrayList;
import java.util.List;

/** Flag-value argv parsing for the worker subcommands. */
final class Args {

    private final String[] args;

    Args(String[] args) {
        this.args = args;
    }

    String required(String flag) {
        String value = optional(flag, null);
        if (value == null) {
            throw new IllegalArgumentException("missing required argument: " + flag);
        }
        return value;
    }

    String optional(String flag, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return fallback;
    }

    List<String> all(String flag) {
        List<String> out = new ArrayList<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(flag)) {
                out.add(args[i + 1]);
            }
        }
        return out;
    }
}
