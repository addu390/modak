package io.modak.worker;

/**
 * Entrypoint for the worker binary. {@code run} (default) hosts the tiering +
 * compaction daemon; {@code register}/{@code unregister} onboard and offboard
 * tables; {@code verify} audits heap-vs-lake consistency. All wiring comes from
 * env vars (see {@link WorkerConfig}).
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) throws Exception {
        WorkerConfig config = WorkerConfig.fromEnv(System.getenv());
        String command = args.length > 0 ? args[0] : "run";
        switch (command) {
            case "run" -> {
                new WorkerDaemon(config).start();
                Thread.currentThread().join(); // scheduler thread keeps the JVM alive
            }
            case "register" -> TableRegistrar.run(config, args);
            case "unregister" -> TableUnregistrar.run(config, args);
            case "verify" -> System.exit(TableVerifier.run(config, args));
            default -> {
                System.err.println("""
                        usage: modak-worker [run]
                               modak-worker register --table <schema.table> --pk <col>[,<col>...] --tier-key <col>
                                                     [--mode tiered|mirrored] [--heap-retention <n>]
                                                     [--lake-retention <n>] [--partition-width <n>]
                               modak-worker unregister --table <schema.table> [--drop-lake]
                               modak-worker verify --table <schema.table>
                        """);
                System.exit(2);
            }
        }
    }
}
