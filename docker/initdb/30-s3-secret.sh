#!/bin/bash
# DuckDB secrets for the read path: TIERDB_S3_* makes the default unscoped
# secret; each TIERDB_READ_SECRET_<NAME> (';'-separated key=value pairs passed
# through to duckdb.create_simple_secret) makes a scoped one per profile.
set -euo pipefail

if [ -n "${TIERDB_S3_ENDPOINT:-}" ]; then
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<EOSQL
SELECT duckdb.create_simple_secret(
    type      := 'S3',
    key_id    := '${TIERDB_S3_ACCESS_KEY:-}',
    secret    := '${TIERDB_S3_SECRET_KEY:-}',
    region    := '${TIERDB_S3_REGION:-us-east-1}',
    url_style := 'path',
    endpoint  := '${TIERDB_S3_ENDPOINT}',
    use_ssl   := '${TIERDB_S3_USE_SSL:-false}'
);
EOSQL
else
    echo "TIERDB_S3_ENDPOINT not set, skipping the default DuckDB secret"
fi

for var in $(compgen -e | grep '^TIERDB_READ_SECRET_' || true); do
    args=""
    IFS=';' read -ra pairs <<< "${!var}"
    for pair in "${pairs[@]}"; do
        [ -z "$pair" ] && continue
        key="${pair%%=*}"
        val="${pair#*=}"
        args+="${args:+, }${key} := '${val//\'/\'\'}'"
    done
    if [ -n "$args" ]; then
        echo "creating scoped DuckDB secret from ${var}"
        psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
            -c "SELECT duckdb.create_simple_secret(${args});"
    fi
done
