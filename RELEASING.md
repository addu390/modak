# Releasing

Releases are cut from a version tag. Pushing `vX.Y.Z` runs `.github/workflows/release.yml`, which packages the extension for Postgres 16 and 17 on x86_64 and arm64 Linux, builds the worker and console jars, builds the `tierdb-embedded` bundle (console jar plus a self-contained jlink runtime, for embedded worker mode), and attaches everything to a GitHub release.

## Cutting a release

1. Bump the extension version in `extension/Cargo.toml` (`workspace.package.version`) and refresh the lockfile with `cargo update -w` from `extension/`.
2. Bump the worker to the same version by editing the `<revision>` property in `worker/pom.xml`.
3. If the extension's SQL surface changed since the last release, add an upgrade script (see below).
4. Commit, tag, push:

```bash
git tag vX.Y.Z
git push origin main vX.Y.Z
```

## Extension upgrade scripts

Installed clusters upgrade with `ALTER EXTENSION tierdb UPDATE`, and Postgres walks a chain of upgrade scripts to get there. Every release that changes the extension's SQL surface (functions, views, GUC-registering init code) must ship `extension/crates/tierdb-pg/sql/tierdb--OLD--NEW.sql` containing just the statements that take an OLD install to NEW. Releases that only change Rust internals still need an empty upgrade script so the chain stays unbroken.

The catalog schema (`sql/catalog.sql`) versions separately. The worker applies it at startup, so it needs no extension upgrade script.

## Installing a packaged build

See [Installation](docs/getting-started/installation.md) for the full manual setup: the tarball extract, postgresql.conf, the DuckDB extensions and S3 secret, and running the worker.
