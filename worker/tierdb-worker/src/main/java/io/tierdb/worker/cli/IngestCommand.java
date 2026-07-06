package io.tierdb.worker.cli;

import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.LakeStorage;
import io.tierdb.worker.LakeStorages;
import io.tierdb.worker.WorkerConfig;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@code tierdb-worker ingest} entry point: parses argv, wires the catalog
 * and the lake, and hands off to {@link IngestOperation}.
 */
public final class IngestCommand {

    private IngestCommand() {}

    public static void run(WorkerConfig config, String[] args) throws Exception {
        String qualified = new Args(args).required("--table");
        String[] parts = qualified.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("--table must be schema-qualified: " + qualified);
        }
        JdbcCatalog catalog = new JdbcCatalog(config.dataSource());
        RegisteredTable table = catalog.lookup(parts[0], parts[1])
                .orElseThrow(() -> new IllegalArgumentException(
                        qualified + " is not registered in tierdb.tables"));
        run(config, args, new LakeStorages(config, catalog).forTable(table));
    }

    public static void run(WorkerConfig config, String[] args, LakeStorage lake) throws Exception {
        Args parsed = new Args(args);
        String qualified = parsed.required("--table");
        List<String> files = parsed.all("--file");
        String jsonl = parsed.optional("--jsonl", null);
        if (files.isEmpty() && jsonl == null) {
            throw new IllegalArgumentException("missing required argument: --file or --jsonl");
        }
        String[] parts = qualified.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("--table must be schema-qualified: " + qualified);
        }

        JdbcCatalog catalog = new JdbcCatalog(config.dataSource());
        RegisteredTable table = catalog.lookup(parts[0], parts[1])
                .orElseThrow(() -> new IllegalArgumentException(
                        qualified + " is not registered in tierdb.tables"));

        IngestOperation operation = new IngestOperation(catalog, lake);
        if (jsonl != null) {
            List<Column> columns = TableRegistrar.columnsOf(
                    config.dataSource(), parts[0], parts[1]);
            files = new ArrayList<>(files);
            files.addAll(operation.stage(table, columns, Path.of(jsonl)));
        }
        operation.ingest(table, files);
    }
}
