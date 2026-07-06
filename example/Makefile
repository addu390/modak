# Example sandbox lifecycle (compose/*.yml, docker compose). Run from here as
# `make -C example up` or `cd example && make up`.
#
# Which overlays are up lives in .env's COMPOSE_FILE, which docker compose
# reads automatically, so add/remove/patch work across separate make
# invocations without tearing down what is already running. The project name
# is pinned to tierdb so this lines up with COMPOSE_FILE set directly by
# lib.sh/run.sh from the repo root, both drive the same stack.

export COMPOSE_PROJECT_NAME := tierdb

.DEFAULT_GOAL := help

.PHONY: up
up: ## Bring up the example sandbox (EXAMPLE_EMBEDDED=1 for embedded mode)
	@if [ "$(EXAMPLE_EMBEDDED)" = "1" ]; then \
		echo "COMPOSE_FILE=compose/tierdb-embedded.yml:compose/rustfs.yml:compose/rustfs-embedded.yml" > .env; \
	else \
		echo "COMPOSE_FILE=compose/tierdb-standalone.yml:compose/rustfs.yml" > .env; \
	fi
	docker compose up -d --build

.PHONY: down
down: ## Tear down the example sandbox and remove all data
	@test -f .env && docker compose down -v || true
	rm -f .env

.PHONY: patch
patch: ## Rebuild images and restart changed services, no teardown
	docker compose up -d --build

.PHONY: status
status: ## Show which overlays are currently layered in
	@test -f .env && grep '^COMPOSE_FILE=' .env | cut -d= -f2- | tr ':' '\n' || echo "example is down (make up)"

.PHONY: add-%
add-%: ## Layer in an overlay without touching what is already up, e.g. make add-trino
	@test -f .env || { echo "example is down, run make up first"; exit 1; }
	@current=$$(grep '^COMPOSE_FILE=' .env | cut -d= -f2-); \
	layer=compose/$*.yml; \
	test -f $$layer || { echo "no such overlay: $$layer"; exit 1; }; \
	case ":$$current:" in \
		*":$$layer:"*) ;; \
		*) echo "COMPOSE_FILE=$$current:$$layer" > .env ;; \
	esac
	docker compose up -d --build

.PHONY: remove-%
remove-%: ## Drop an overlay and stop its services, e.g. make remove-trino
	@test -f .env || { echo "example is down, run make up first"; exit 1; }
	@current=$$(grep '^COMPOSE_FILE=' .env | cut -d= -f2-); \
	layer=compose/$*.yml; \
	new=$$(printf '%s' "$$current" | tr ':' '\n' | grep -v "^$$layer$$" | paste -sd: -); \
	echo "COMPOSE_FILE=$$new" > .env
	docker compose up -d --remove-orphans

.PHONY: scenario-%
scenario-%: ## Run one scenario against whatever is up, e.g. make scenario-core
	./scenarios/$*.sh

.PHONY: scenarios
scenarios: ## Run every scenario (run.sh), trino/spark skip unless their overlay is up
	./scenarios/run.sh

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nExample targets:\n\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2 }' Makefile
	@echo ""
