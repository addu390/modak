package io.modak.lake.iceberg;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopTables;

/**
 * Resolves {@code lake_table_ref} to an Iceberg {@link Table}: a warehouse path
 * ({@link HadoopTables}) by default, or a {@code namespace.table} REST-catalog
 * identifier when the config carries {@code catalog.uri}. {@code iceberg.catalog.*}
 * and {@code iceberg.table.*} config keys pass through verbatim.
 */
public final class IcebergTables {

    private static final String CATALOG_PREFIX = "iceberg.catalog.";
    private static final String TABLE_PREFIX = "iceberg.table.";

    private final Configuration conf;
    private final Catalog catalog;
    private final Map<String, String> tableProps;

    private IcebergTables(Configuration conf, Catalog catalog, Map<String, String> tableProps) {
        this.conf = conf;
        this.catalog = catalog;
        this.tableProps = tableProps;
    }

    public static IcebergTables from(Map<String, String> config, Configuration conf) {
        Map<String, String> tableProps = prefixed(config, TABLE_PREFIX);
        String uri = config.get("catalog.uri");
        if (uri == null || uri.isBlank()) {
            return new IcebergTables(conf, null, tableProps);
        }
        Map<String, String> props = new HashMap<>();
        props.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
        props.put(CatalogProperties.URI, uri);
        // IO goes through our Hadoop conf (s3a), not the server-suggested
        // io-impl, the worker doesn't ship iceberg-aws.
        props.put(CatalogProperties.FILE_IO_IMPL,
                config.getOrDefault("catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO"));
        putIfSet(props, CatalogProperties.WAREHOUSE_LOCATION, config.get("catalog.warehouse"));
        putIfSet(props, "token", config.get("catalog.token"));
        props.putAll(prefixed(config, CATALOG_PREFIX));
        return new IcebergTables(conf,
                CatalogUtil.buildIcebergCatalog("modak", props, conf), tableProps);
    }

    private static Map<String, String> prefixed(Map<String, String> config, String prefix) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> e : config.entrySet()) {
            if (e.getKey().startsWith(prefix)) {
                out.put(e.getKey().substring(prefix.length()), e.getValue());
            }
        }
        return Map.copyOf(out);
    }

    public boolean viaCatalog() {
        return catalog != null;
    }

    public Table load(String ref) {
        return catalog == null
                ? new HadoopTables(conf).load(ref)
                : catalog.loadTable(TableIdentifier.parse(ref));
    }

    /** Creates the table when absent (both schemes are idempotent) and returns it. */
    public Table createIfAbsent(String ref, Schema schema, PartitionSpec spec) {
        if (catalog == null) {
            HadoopTables tables = new HadoopTables(conf);
            return tables.exists(ref)
                    ? tables.load(ref)
                    : tables.create(schema, spec, tableProps, ref);
        }
        TableIdentifier id = TableIdentifier.parse(ref);
        ensureNamespace(id.namespace());
        if (catalog.tableExists(id)) {
            return catalog.loadTable(id);
        }
        try {
            return catalog.createTable(id, schema, spec, tableProps);
        } catch (AlreadyExistsException e) {
            return catalog.loadTable(id);
        }
    }

    /** Drops the table with purge (data files deleted), absent is a no-op. */
    public void drop(String ref) {
        if (catalog == null) {
            HadoopTables tables = new HadoopTables(conf);
            if (tables.exists(ref)) {
                tables.dropTable(ref, /*purge=*/ true);
            }
            return;
        }
        TableIdentifier id = TableIdentifier.parse(ref);
        if (catalog.tableExists(id)) {
            catalog.dropTable(id, /*purge=*/ true);
        }
    }

    private void ensureNamespace(Namespace namespace) {
        if (!(catalog instanceof SupportsNamespaces namespaces) || namespace.isEmpty()) {
            return;
        }
        try {
            if (!namespaces.namespaceExists(namespace)) {
                namespaces.createNamespace(namespace);
            }
        } catch (AlreadyExistsException ignored) {
        }
    }

    private static void putIfSet(Map<String, String> props, String key, String value) {
        if (value != null && !value.isBlank()) {
            props.put(key, value);
        }
    }
}
