# Contributing

Thanks for helping out. Bug reports and feature requests go through [issues](https://github.com/Modak-Labs/tierdb/issues), questions and ideas through [discussions](https://github.com/Modak-Labs/tierdb/discussions). For anything non-trivial, open an issue or discussion before writing code so the approach can be agreed on first.

## Local setup

The full stack runs locally with Docker:

```bash
make -C example up
./example/scenarios/run.sh
```

That brings up Postgres with the extension, RustFS, and the worker, then runs the scripted walkthrough. The console lives at http://localhost:9090.

## Building and testing

The extension is a Rust workspace built with [pgrx](https://github.com/pgcentralfoundation/pgrx):

```bash
cd extension
cargo test -p tierdb-core        # pure logic, no Postgres needed
cargo pgrx test pg17 -p tierdb   # in-database tests
```

The worker is a Maven multi-module project:

```bash
mvn -f worker/pom.xml verify
```

Docs are MkDocs Material, `mkdocs serve` from the repo root.

## Sending changes

- Keep pull requests small and focused on one change.
- Run the tests above plus `./example/scenarios/run.sh` against a fresh stack, since the example doubles as the end-to-end suite.
- Follow the surrounding code style, and keep comments and docs plain and concise.

TierDB is MIT licensed. By contributing you agree your work is too.
