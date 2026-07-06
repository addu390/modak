# Integrations

TierDB has two integration surfaces. External engines read the consistent two-tier view through the [seam protocol](../reference/seam.md), and external producers write through [Stream Load](../ingestion/stream-load.md). Everything in this section builds on one of the two.

## Reads: the seam protocol

The worker keeps the seam state in plain catalog tables, so any engine that can reach Postgres and the lake can produce the same consistent view the extension does. That matters most on managed Postgres (RDS, Aurora, Cloud SQL), where the extension cannot be installed and a connector becomes the read path for tiered data.

Connectors share `tierdb-connector`, a small engine-agnostic library covering the pin lifecycle and the atomic seam capture over JDBC. The engine half stays thin: scan the heap above the cut-line, scan the lake at the pinned snapshot, merge the delta.

| Connector | Status |
|-----------|--------|
| [Spark](spark.md) | Available |
| [Trino](trino.md) | Available |
| DuckDB | Planned |
| Athena | Planned |

## Writes: Stream Load

[Stream Load](../ingestion/stream-load.md) is the ingestion counterpart. The `tierdb-load` library gives any JVM process labeled exactly-once micro-batches, and the worker exposes the same path over HTTP for everything else. Kafka Connect, Flink, and Spark Structured Streaming sinks built on it are planned.

Anything not listed can still read the lake directly. TierDB writes standard Iceberg, and a plain snapshot read is consistent, just bounded at the cut-line rather than merged with the heap.
