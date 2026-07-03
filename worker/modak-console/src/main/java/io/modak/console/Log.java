package io.modak.console;

import java.time.Instant;

/** Timestamped stdout logging, deliberately dependency-free, mirrors the worker's. */
final class Log {

    private Log() {}

    static void info(String fmt, Object... args) {
        System.out.printf("%s [modak-console] %s%n", Instant.now(), fmt.formatted(args));
    }

    static void error(String fmt, Object... args) {
        System.err.printf("%s [modak-console] ERROR %s%n", Instant.now(), fmt.formatted(args));
    }
}
