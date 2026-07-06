package io.tierdb.worker;

import java.time.Instant;

/** Timestamped stdout logging, deliberately dependency-free for the worker binary. */
public final class Log {

    private Log() {}

    public static void info(String fmt, Object... args) {
        System.out.printf("%s [tierdb-worker] %s%n", Instant.now(), fmt.formatted(args));
    }

    public static void warn(String fmt, Object... args) {
        System.err.printf("%s [tierdb-worker] WARN %s%n", Instant.now(), fmt.formatted(args));
    }

    public static void error(String fmt, Object... args) {
        System.err.printf("%s [tierdb-worker] ERROR %s%n", Instant.now(), fmt.formatted(args));
    }
}
