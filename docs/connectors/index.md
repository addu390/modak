# Connectors

A connector is an external consumer of the
[seam protocol](../reference/seam.md). The worker keeps the seam state in
plain catalog tables, so any engine that can reach Postgres and the lake can
produce the same consistent two-tier view the extension does. That matters
most on managed Postgres (RDS, Aurora, Cloud SQL), where the extension cannot
be installed and a connector becomes the read path for tiered data.

Connectors share `modak-connector`, a small engine-agnostic library covering
the pin lifecycle and the atomic seam capture over JDBC. The engine half stays
thin: scan the heap above the cut-line, scan the lake at the pinned snapshot,
merge the delta.

| Connector | Status |
|-----------|--------|
| [Spark](spark.md) | Available |
| Trino | Planned |
| DuckDB | Planned |
| Athena | Planned |

On the write side, [Stream Load](../guides/stream-load.md) is the ingestion
counterpart: the `modak-load` library gives any JVM process labeled
exactly-once micro-batches, and the worker exposes it over HTTP for everything
else. Kafka Connect, Flink, and Spark Structured Streaming sinks built on it
are planned.

Anything not listed can still read the lake directly. Modak writes standard
Iceberg, and a plain snapshot read is consistent, just bounded at the
cut-line rather than merged with the heap.
