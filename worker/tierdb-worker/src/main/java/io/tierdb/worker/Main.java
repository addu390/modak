package io.tierdb.worker;

import io.tierdb.worker.cli.IngestCommand;
import io.tierdb.worker.cli.MaintainCommand;
import io.tierdb.worker.cli.PolicyCommand;
import io.tierdb.worker.cli.ProfileCommand;
import io.tierdb.worker.cli.TableRegistrar;
import io.tierdb.worker.cli.TableUnregistrar;
import io.tierdb.worker.cli.TableVerifier;

/** Entrypoint for the worker binary. */
public final class Main {

    private Main() {}

    public static void main(String[] args) throws Exception {
        WorkerConfig config = WorkerConfig.fromEnv(System.getenv());
        String command = args.length > 0 ? args[0] : "run";
        switch (command) {
            case "run" -> {
                WorkerDaemon daemon = new WorkerDaemon(config);
                daemon.start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        daemon.stop();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }, "tierdb-shutdown"));
                Thread.currentThread().join();
            }
            case "register" -> TableRegistrar.run(config, args);
            case "unregister" -> TableUnregistrar.run(config, args);
            case "verify" -> System.exit(TableVerifier.run(config, args));
            case "ingest" -> IngestCommand.run(config, args);
            case "policy" -> PolicyCommand.run(config, args);
            case "maintain" -> System.exit(MaintainCommand.run(config, args));
            case "profile" -> ProfileCommand.run(config, args);
            default -> {
                System.err.println("""
                        usage: tierdb-worker [run]
                               tierdb-worker register --table <schema.table> --pk <col>[,<col>...] --tier-key <col>
                                                     [--mode tiered|mirrored] [--heap-retention <n>]
                                                     [--lake-retention <n>] [--partition-width <n>]
                                                     [--keep-heap] [--profile <name>]
                               tierdb-worker unregister --table <schema.table> [--drop-lake]
                               tierdb-worker verify --table <schema.table>
                               tierdb-worker ingest --table <schema.table> [--file <parquet>...] [--jsonl <file>]
                               tierdb-worker policy --table <schema.table> [--set <key=value>...]
                                                   [--unset <key>...] [--reset]
                               tierdb-worker maintain --table <schema.table> [--no-wait]
                               tierdb-worker profile list
                               tierdb-worker profile create --name <name> --warehouse <root>
                                                           [--format <plugin>] [--config <key=value;...>]
                                                           [--credentials <ref>] [--default]
                        """);
                System.exit(2);
            }
        }
    }
}
