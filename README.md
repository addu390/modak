# Modak

Modak makes a Postgres table and an Apache Iceberg table behave as one table.

> **Status: beta.** No stable release yet, interfaces can still change. See [production deployment](https://addu390.github.io/modak/guides/production/) before running it anywhere that matters.

Recent rows live in Postgres, history lives in Iceberg, and plain SQL works
against the whole timeline: `SELECT`, `INSERT`, `UPDATE`, and `DELETE` reach
any row, wherever it lives, with transactional-grade consistency. Both tiers
stay real, independently usable systems, an unforked Postgres you run OLTP on
and a standard Iceberg warehouse any engine can read. Modak owns only the seam
between them: a cut-line, a pinned snapshot, and a correction delta merged on
read. The [protocol](https://addu390.github.io/modak/reference/seam/) is
public.

Tables run **tiered** (Postgres keeps only the recent partitions) or  
**mirrored** (Postgres keeps everything while CDC trails it into the lake).  
[The contract](https://addu390.github.io/modak/getting-started/contract/)  
states exactly what each mode supports.

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
- [The contract](https://addu390.github.io/modak/getting-started/contract/): the mode-by-mode matrix of what you can read, write, and correct
- [Guides](https://addu390.github.io/modak/guides/registering-tables/): registering tables, reading, corrections, operations
- [Reference](https://addu390.github.io/modak/reference/sql/): SQL API, CLI, configuration, catalog schema, metrics
- [Architecture](https://addu390.github.io/modak/architecture/): how the extension, the worker, and the catalog cooperate

## License

Apache-2.0. See `[LICENSE](LICENSE)`.