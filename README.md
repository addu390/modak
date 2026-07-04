# Modak

Tier-aware data federation between Postgres and Apache Iceberg. Modak knows which tier holds what, and every query stays real time and consistent.

> **Status: beta.** No stable release yet, interfaces can still change. See [production deployment](https://modak-labs.github.io/modak/operations/production/) before running it anywhere that matters.

Recent rows live in Postgres, history lives in Iceberg, and plain SQL works against the whole timeline: `SELECT`, `INSERT`, `UPDATE`, and `DELETE` reach any row, wherever it lives. Both tiers stay real, independently usable systems: an unforked Postgres you run OLTP on, and a standard Iceberg warehouse any engine can read. Modak owns only the seam between them, and the [protocol](https://modak-labs.github.io/modak/reference/seam/) is public.

https://github.com/user-attachments/assets/09966acf-b3d7-4a29-bd57-12bad806772d


Tables run **tiered** (Postgres keeps only the recent partitions) or **mirrored** (Postgres keeps everything while CDC trails it into the lake). [Choosing a mode](https://modak-labs.github.io/modak/modes/choosing/) walks the decision, and [the contract](https://modak-labs.github.io/modak/modes/contract/) states exactly what each mode supports.

## Installation

Run the full loop locally with Docker:

```bash
git clone --recurse-submodules https://github.com/Modak-Labs/modak && cd modak
docker compose up -d --build
./example/run.sh
```

That brings up Postgres with the extension, MinIO as the Iceberg warehouse, and the worker, then walks through tiering, corrections, mirroring, and lifecycle end to end. The console lives at [http://localhost:9090](http://localhost:9090).

For the guided version, start with the [quickstart](https://modak-labs.github.io/modak/getting-started/quickstart/). For pointing the worker at your own Postgres and object store, see [production deployment](https://modak-labs.github.io/modak/operations/production/).

## Documentation

Full docs at [modak-labs.github.io/modak](https://modak-labs.github.io/modak/):

- [Concepts](https://modak-labs.github.io/modak/getting-started/concepts/): table modes, tier key, cut-line, pinned snapshot, delta
- [Choosing a mode](https://modak-labs.github.io/modak/modes/choosing/): which mode fits your workload
- [The contract](https://modak-labs.github.io/modak/modes/contract/): the mode-by-mode matrix of what you can read, write, and correct
- [Working with tables](https://modak-labs.github.io/modak/tables/registering-tables/): registering, reading, corrections
- [Ingestion](https://modak-labs.github.io/modak/ingestion/bulk-ingestion/): bulk ingestion and Stream Load
- [Operations](https://modak-labs.github.io/modak/operations/production/): production deployment, AWS, day-2 operations, console
- [Reference](https://modak-labs.github.io/modak/reference/sql/): SQL API, CLI, configuration, catalog schema, metrics
- [Architecture](https://modak-labs.github.io/modak/getting-started/architecture/): how the extension, the worker, and the catalog cooperate

The explainer and demo videos, and the toolchain that builds them, live in [Modak-Labs/modak-media](https://github.com/Modak-Labs/modak-media).

## Contributing

Bug reports and feature requests go through [issues](https://github.com/Modak-Labs/modak/issues), questions and ideas through [discussions](https://github.com/Modak-Labs/modak/discussions). See [`CONTRIBUTING.md`](CONTRIBUTING.md) for the local setup and how to send changes.

## License

MIT. See [`LICENSE`](LICENSE).
