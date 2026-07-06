# Shared helpers for the example steps. Source, don't execute.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ "${EXAMPLE_EMBEDDED:-0}" = "1" ]; then
    COMPOSE_FILE=example/compose/tierdb-embedded.yml:example/compose/rustfs.yml:example/compose/rustfs-embedded.yml
else
    COMPOSE_FILE=example/compose/tierdb-standalone.yml:example/compose/rustfs.yml
fi

if [ "${EXAMPLE_CATALOG:-0}" = "1" ]; then
    COMPOSE_FILE="$COMPOSE_FILE:example/compose/lakekeeper.yml"
fi

if [ "${EXAMPLE_TRINO:-0}" = "1" ]; then
    COMPOSE_FILE="$COMPOSE_FILE:example/compose/trino.yml"
fi

if [ "${EXAMPLE_SPARK:-0}" = "1" ]; then
    COMPOSE_FILE="$COMPOSE_FILE:example/compose/spark.yml"
fi

export COMPOSE_FILE
export COMPOSE_PROJECT_NAME=tierdb

PSQL="docker compose exec -T postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 -X -q"

say() { echo "== $* =="; }

fail() {
    echo "EXAMPLE FAIL: $*" >&2
    exit 1
}

assert_eq() {
    if [ "$2" != "$3" ]; then
        echo "EXAMPLE FAIL: $1" >&2
        echo "--- expected:" >&2; echo "$2" >&2
        echo "--- got:" >&2; echo "$3" >&2
        exit 1
    fi
}

wait_for() {
    local label=$1 sql=$2 expected=$3
    for _ in $(seq 1 60); do
        if [ "$($PSQL -tA -c "$sql")" = "$expected" ]; then
            echo "   ... $label"
            return 0
        fi
        sleep 2
    done
    fail "timeout waiting for: $label"
}

preflight() {
    if [ "$($PSQL -tA -c 'SHOW wal_level')" != "logical" ]; then
        echo "wal_level is not 'logical', the data volume predates mirrored-table support." >&2
        echo "Recreate the stack:  make -C example down && make -C example up" >&2
        exit 1
    fi
    if [ "${EXAMPLE_CATALOG:-0}" = "1" ]; then
        docker compose run --rm --entrypoint /bin/sh rustfs-init -c \
            "rc alias set local http://rustfs:9000 rustfs-root-user rustfs-root-password >/dev/null \
             && rc rm -r --force local/warehouse/rest >/dev/null 2>&1; true"
    fi
}

reset_table() {
    docker compose run --rm worker unregister --table "public.$1" --drop-lake
    $PSQL -c "DROP TABLE IF EXISTS public.$1" >/dev/null
}
