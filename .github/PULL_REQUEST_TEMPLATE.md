## What

What this changes and why.

## How it was tested

Which of these ran clean: `cargo test -p tierdb-core`, `cargo pgrx test pg17 -p tierdb`, `mvn -f worker/pom.xml verify`, `./example/scenarios/run.sh` (against a stack built from this branch: `make -C example patch`, not the pulled images).
