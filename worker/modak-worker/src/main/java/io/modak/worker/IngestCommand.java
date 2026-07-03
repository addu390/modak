package io.modak.worker;

import io.modak.catalog.JdbcCatalog;
import io.modak.catalog.RegisteredTable;
import io.modak.common.RowBatchData.Column;
import io.modak.lake.LakeStorage;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@code modak-worker ingest} entry point: parses argv, wires the catalog
 * and the lake, and hands off to {@link IngestOperation}.
 */
final class IngestCommand {

    private IngestCommand() {}

    static void run(WorkerConfig config, String[] args) throws Exception {
        run(config, args, LakePlugins.load(config.lakeFormat(), config.lakeConfig()));
    }

    static void run(WorkerConfig config, String[] args, LakeStorage lake) throws Exception {
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
                        qualified + " is not registered in modak.tables"));

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
