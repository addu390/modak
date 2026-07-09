# TierDB

TierDB is a fast, transparent and cost-effective way to tier Postgres data into Apache Iceberg. If Postgres sits at the heart of your stack and its largest tables keep growing (events, telemetry, orders, time series), TierDB keeps the recent rows hot in Postgres, moves history into the lake, and your application keeps running plain SQL against one table as if nothing moved.

> **Status: beta.** No stable release yet, interfaces can still change. See [production deployment](https://modak-labs.github.io/tierdb/operations/production/) before running it anywhere that matters.

Tables run in two modes: **tiered**, where Postgres keeps only the recent partitions and history lives in Iceberg, and **mirrored**, where Postgres keeps everything while CDC trails every change into the lake. With the postgres extension, plain SQL reads and writes both tiers in place, and where it cannot be (managed Postgres like RDS), tiering still runs and [connectors](https://modak-labs.github.io/tierdb/integrations/) cover the cross-tier side.

It is built on guarantees rather than best effort: writes stay ACID wherever the row lives, every read is one point-in-time view across both tiers, and failure degrades to lag, never to a wrong answer.

TierDB owns only the seam between the tiers, and the [protocol](https://modak-labs.github.io/tierdb/reference/seam/) is public. [Choosing a mode](https://modak-labs.github.io/tierdb/modes/choosing/) walks the decision, and [the contract](https://modak-labs.github.io/tierdb/modes/contract/) states exactly what each mode supports.

## Installation

Run the full loop locally with Docker:

```bash
git clone --recurse-submodules https://github.com/Modak-Labs/tierdb && cd tierdb
make -C example up
./example/scenarios/run.sh
```

That brings up Postgres with the extension, RustFS as the Iceberg warehouse, and the worker, then walks through tiering, corrections, mirroring, and lifecycle end to end. The console lives at [http://localhost:9090](http://localhost:9090).

For the guided version, start with the [quickstart](https://modak-labs.github.io/tierdb/getting-started/quickstart/). For pointing the worker at your own Postgres and object store, see [production deployment](https://modak-labs.github.io/tierdb/operations/production/).

## Documentation

Full docs at [modak-labs.github.io/tierdb](https://modak-labs.github.io/tierdb/):

- [Concepts](https://modak-labs.github.io/tierdb/getting-started/concepts/): table modes, tier key, cut-line, pinned snapshot, delta
- [Choosing a mode](https://modak-labs.github.io/tierdb/modes/choosing/): which mode fits your workload
- [The contract](https://modak-labs.github.io/tierdb/modes/contract/): the mode-by-mode matrix of what you can read, write, and correct
- [Working with tables](https://modak-labs.github.io/tierdb/tables/registering-tables/): registering, reading, corrections
- [Ingestion](https://modak-labs.github.io/tierdb/ingestion/bulk-ingestion/): bulk ingestion and Stream Load
- [Operations](https://modak-labs.github.io/tierdb/operations/production/): production deployment, AWS, day-2 operations, console
- [Reference](https://modak-labs.github.io/tierdb/reference/sql/): SQL API, CLI, configuration, catalog schema, metrics
- [Architecture](https://modak-labs.github.io/tierdb/getting-started/architecture/): how the extension, the worker, and the catalog cooperate

## Contributing

Bug reports and feature requests go through [issues](https://github.com/Modak-Labs/tierdb/issues), questions and ideas through [discussions](https://github.com/Modak-Labs/tierdb/discussions). See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the local setup and how to send changes.

## License

MIT. See [`LICENSE`](LICENSE).
