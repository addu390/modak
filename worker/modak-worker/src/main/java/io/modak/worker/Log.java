package io.modak.worker;

import java.time.Instant;

/** Timestamped stdout logging, deliberately dependency-free for the worker binary. */
final class Log {

    private Log() {}

    static void info(String fmt, Object... args) {
        System.out.printf("%s [modak-worker] %s%n", Instant.now(), fmt.formatted(args));
    }

    static void warn(String fmt, Object... args) {
        System.err.printf("%s [modak-worker] WARN %s%n", Instant.now(), fmt.formatted(args));
    }

    static void error(String fmt, Object... args) {
        System.err.printf("%s [modak-worker] ERROR %s%n", Instant.now(), fmt.formatted(args));
    }
}
