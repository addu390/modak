# TierDB build & test harness.
#
# Two deployable halves: the Postgres extension (extension/, a Rust workspace)
# and the worker fleet (worker/, a Java Maven reactor). Targets are split per
# half and composed by the aggregate targets. Run `make` or `make help`.

EXT_DIR    := extension
WORKER_DIR := worker

# Quiet Maven; override with `make MVN_FLAGS=` for full output.
MVN       := mvn
MVN_FLAGS := -q
CARGO     := cargo

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Aggregate targets
# ---------------------------------------------------------------------------

.PHONY: build
build: build-extension build-worker ## Compile both halves

.PHONY: test
test: test-extension test-worker ## Run all tests

.PHONY: verify
verify: check-extension test-extension verify-worker ## Full gate: fmt+clippy+test (extension), verify (worker)

.PHONY: fmt
fmt: fmt-extension ## Auto-format (extension)

.PHONY: clean
clean: clean-extension clean-worker ## Remove build artifacts on both halves

# ---------------------------------------------------------------------------
# Postgres extension (extension/)
# ---------------------------------------------------------------------------

.PHONY: build-extension
build-extension: ## cargo check the workspace
	cd $(EXT_DIR) && $(CARGO) check --workspace

.PHONY: test-extension
test-extension: ## cargo test the pure crates (no Postgres; excludes the pgrx crate)
	cd $(EXT_DIR) && $(CARGO) test --workspace --exclude tierdb

.PHONY: check-extension
check-extension: ## rustfmt --check + clippy (deny warnings)
	cd $(EXT_DIR) && $(CARGO) fmt --all -- --check
	cd $(EXT_DIR) && $(CARGO) clippy --workspace --all-targets -- -D warnings

.PHONY: fmt-extension
fmt-extension: ## rustfmt (write)
	cd $(EXT_DIR) && $(CARGO) fmt --all

.PHONY: pg-test
pg-test: ## Run pgrx integration tests in a temp Postgres (needs cargo-pgrx + `cargo pgrx init`)
	cd $(EXT_DIR) && LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 $(CARGO) test -p tierdb --features pg_test

.PHONY: clean-extension
clean-extension:
	cd $(EXT_DIR) && $(CARGO) clean

# ---------------------------------------------------------------------------
# Worker fleet (worker/)
# ---------------------------------------------------------------------------

.PHONY: build-worker
build-worker: ## mvn compile the reactor
	cd $(WORKER_DIR) && $(MVN) $(MVN_FLAGS) -DskipTests test-compile

.PHONY: test-worker
test-worker: ## mvn test the reactor
	cd $(WORKER_DIR) && $(MVN) $(MVN_FLAGS) test

.PHONY: verify-worker
verify-worker: ## mvn verify the reactor
	cd $(WORKER_DIR) && $(MVN) $(MVN_FLAGS) verify

.PHONY: clean-worker
clean-worker:
	cd $(WORKER_DIR) && $(MVN) $(MVN_FLAGS) clean

.PHONY: package-embedded
package-embedded: ## Console jar + jlink runtime bundle for embedded mode (worker/target/tierdb-embedded)
	cd $(WORKER_DIR) && $(MVN) $(MVN_FLAGS) -DskipTests package
	rm -rf $(WORKER_DIR)/target/tierdb-embedded
	mkdir -p $(WORKER_DIR)/target/tierdb-embedded
	jlink --add-modules "$$(cat $(WORKER_DIR)/jlink-modules.txt)" \
		--strip-debug --no-man-pages --no-header-files --compress=zip-6 \
		--output $(WORKER_DIR)/target/tierdb-embedded/runtime
	cp $(WORKER_DIR)/tierdb-console/target/tierdb-console.jar $(WORKER_DIR)/target/tierdb-embedded/

# ---------------------------------------------------------------------------
# Meta
# ---------------------------------------------------------------------------

.PHONY: tools
tools: ## Report which toolchains are present
	@printf 'cargo : '; command -v $(CARGO) >/dev/null 2>&1 && $(CARGO) --version || echo 'NOT INSTALLED (https://rustup.rs)'
	@printf 'mvn   : '; command -v $(MVN)   >/dev/null 2>&1 && ($(MVN) --version | head -1) || echo 'NOT INSTALLED'
	@printf 'java  : '; command -v java     >/dev/null 2>&1 && (java -version 2>&1 | head -1) || echo 'NOT INSTALLED'

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nTierDB targets:\n\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 } \
		/^# -+$$/ { }' $(MAKEFILE_LIST)
	@echo ""
