package io.tierdb.worker.cli;

import java.util.ArrayList;
import java.util.List;

/** Flag-value argv parsing for the worker subcommands. */
public final class Args {

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

    boolean has(String flag) {
        for (String arg : args) {
            if (arg.equals(flag)) {
                return true;
            }
        }
        return false;
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
