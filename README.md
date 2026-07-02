# Modak

Modak lets Postgres be the bounded, transactional hot tier of a table whose colder history lives in an open lakehouse (Iceberg), and queries across both tiers as one logical table with transactional-grade read consistency.

- A **tiered** table keeps only its recent partitions in Postgres and moves the rest to Iceberg. A **mirrored** table keeps the full copy in Postgres while CDC trails every change into the lake, optionally shedding heap history it no longer needs hot.
- Either way, a thin open seam (a monotonic cut-line, a pinned lake snapshot, and a PK-keyed correction delta merged on read) stitches the tiers so every query sees a consistent point-in-time view, no duplicates and no gaps. Both tiers stay real, independently usable open systems: a Postgres you can run OLTP on, an Iceberg any engine can read. Modak owns only the glue.

https://github.com/user-attachments/assets/510d60a5-c7e2-4c58-9db8-c7deea79ea8c

## Installation

Run the full loop locally with Docker:

```bash
git clone https://github.com/addu390/modak && cd modak
docker compose up -d --build
./example/run.sh
```

That brings up Postgres with the extension, MinIO as the Iceberg warehouse, and the worker, then walks through tiering, corrections, mirroring, and lifecycle end to end. The console lives at [http://localhost:9090](http://localhost:9090).

For the guided version, start with the [quickstart](https://addu390.github.io/modak/getting-started/quickstart/). For pointing the worker at your own Postgres and object store, see [production deployment](https://addu390.github.io/modak/guides/production/).

## Documentation

Full docs at [addu390.github.io/modak](https://addu390.github.io/modak/):

- [Concepts](https://addu390.github.io/modak/getting-started/concepts/): table modes, tier key, cut-line, pinned snapshot, delta
- [Guides](https://addu390.github.io/modak/guides/registering-tables/): registering tables, reading, corrections, operations
- [Reference](https://addu390.github.io/modak/reference/sql/): SQL API, CLI, configuration, catalog schema, metrics
- [Architecture](https://addu390.github.io/modak/architecture/): how the extension, the worker, and the catalog cooperate

## License

Apache-2.0. See [`LICENSE`](LICENSE).
