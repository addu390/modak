# Trino

`tierdb-trino` is a Trino connector, install it as a plugin and point one catalog at your Postgres and your object store. It gives Trino the same consistent view the extension gives Postgres itself: the hot branch from the heap, the cold branch from the lake at the pinned snapshot, the delta overlay merged newest-wins, predicate pushdown on both branches.

## Installing the plugin

Build the plugin directory and copy it into Trino's plugin directory on every coordinator and worker node:

```bash
mvn -pl connectors/tierdb-trino -am package
cp -r connectors/tierdb-trino/target/plugin/tierdb "$TRINO_HOME/plugin/tierdb"
```

## Catalog configuration

Add a catalog properties file, `etc/catalog/tierdb.properties`:

```properties
connector.name=tierdb
tierdb.jdbc-url=jdbc:postgresql://your-postgres-host:5432/app
tierdb.jdbc-user=tierdb_worker
tierdb.jdbc-password=...
tierdb.fileio.s3.endpoint=https://s3.your-region.amazonaws.com
tierdb.fileio.s3.access-key-id=...
tierdb.fileio.s3.secret-access-key=...
tierdb.fileio.client.region=us-east-1
```

| Property | Required | Meaning |
|----------|----------|---------|
| `tierdb.jdbc-url` | yes | The same Postgres the worker and the extension use. |
| `tierdb.jdbc-user`, `tierdb.jdbc-password` | no | A read-only role is enough, see [Roles and grants](../operations/production.md#roles-and-grants). |
| `tierdb.pin-ttl-seconds` | no | Read pin lifetime, default 900. |
| `tierdb.fileio.*` | for path-based tables | Passed through verbatim to the Iceberg `S3FileIO` client, stripped of the prefix (`tierdb.fileio.s3.endpoint` becomes `s3.endpoint`, and so on). Not needed when tables are catalog-based (`TIERDB_CATALOG_URI` set on the worker), the location is then resolved through that catalog instead. |

Restart Trino, then query any registered table under the `tierdb` catalog like any other table:

```sql
SELECT * FROM tierdb.public.trip_events ORDER BY id;
```

## Behavior

Reads are pinned for the lifetime of the query's split scheduling, the same `tierdb.read_pins` mechanism the extension and `tierdb-spark` use, so lake maintenance holds back snapshot expiry until the query finishes.

Predicate pushdown works on both branches: heap predicates become a `WHERE` clause over JDBC, lake predicates become an Iceberg filter expression. Corrections to cold rows show up immediately through the delta overlay, merged the same way as a plain Postgres read.

Column types unsupported by the connector fail loudly at query planning time rather than silently coercing, see [`HeapCatalog`](https://github.com/Modak-Labs/tierdb/blob/main/connectors/tierdb-connector/src/main/java/io/tierdb/connector/source/HeapCatalog.java) for the supported type list.

Writes are not yet supported through the connector, use the extension's SQL surface, `tierdb-load`, or `tierdb-spark` for writes, and read from Trino.
