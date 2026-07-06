package io.tierdb.console;

import java.time.Instant;

/** Timestamped stdout logging, deliberately dependency-free, mirrors the worker's. */
final class Log {

    private Log() {}

    static void info(String fmt, Object... args) {
        System.out.printf("%s [tierdb-console] %s%n", Instant.now(), fmt.formatted(args));
    }

    static void error(String fmt, Object... args) {
        System.err.printf("%s [tierdb-console] ERROR %s%n", Instant.now(), fmt.formatted(args));
    }
}
