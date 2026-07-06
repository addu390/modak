#!/usr/bin/env bash
# Applies one dataset file to a table. Schemas are DDL, datasets are JSON
# lines, this is the only place that turns a dataset file into rows. Scenario
# scripts never touch SQL for data, they call this with a mode.
#
# Usage: ingest.sh <table> <file> <mode> [flags]
#
# Modes:
#   insert                       bulk insert of full rows
#   update    --pk col           plain UPDATE per row, matched on pk, present columns are the SET list
#   delete    --pk col           plain DELETE per row, matched on pk
#   tierdb-upsert                 tierdb_upsert() per row, tier key lives in the row
#   tierdb-delete --pk col --tier-key col [--tier-key-type type]
#                                 tierdb_delete() per row from the named pk/tier-key fields
#   stream-load --label name [--token tok]
#                                 POST the file as one labeled HTTP batch
#   sql                          run the file directly, for generated data that is not row-shaped
set -euo pipefail
source "$(dirname "$0")/lib.sh"

table=$1
file=$2
mode=$3
shift 3

pk=id
tier_key=
tier_key_type=bigint
label=
token=tierdb-example

while [ $# -gt 0 ]; do
    case "$1" in
        --pk) pk=$2; shift 2 ;;
        --tier-key) tier_key=$2; shift 2 ;;
        --tier-key-type) tier_key_type=$2; shift 2 ;;
        --label) label=$2; shift 2 ;;
        --token) token=$2; shift 2 ;;
        *) fail "unknown flag: $1" ;;
    esac
done

case "$mode" in
insert)
    batch="[$(paste -sd, "$file")]"
    $PSQL -v batch="$batch" <<SQL
INSERT INTO public.$table SELECT * FROM jsonb_populate_recordset(null::public.$table, :'batch'::jsonb);
SQL
    ;;
update)
    batch="[$(paste -sd, "$file")]"
    $PSQL -v batch="$batch" <<SQL
CREATE TEMP TABLE _ingest_batch AS SELECT :'batch'::jsonb AS batch;
DO \$do\$
DECLARE
    r jsonb;
    set_clause text;
BEGIN
    FOR r IN SELECT * FROM jsonb_array_elements((SELECT batch FROM _ingest_batch)) LOOP
        SELECT string_agg(format('%I = %L', key, value), ', ') INTO set_clause
        FROM jsonb_each_text(r - '$pk');
        EXECUTE format('UPDATE public.$table SET %s WHERE %I = %L', set_clause, '$pk', r ->> '$pk');
    END LOOP;
END
\$do\$;
DROP TABLE _ingest_batch;
SQL
    ;;
delete)
    batch="[$(paste -sd, "$file")]"
    $PSQL -v batch="$batch" <<SQL
DELETE FROM public.$table WHERE $pk::text IN (SELECT elem ->> '$pk' FROM jsonb_array_elements(:'batch'::jsonb) elem);
SQL
    ;;
tierdb-upsert)
    batch="[$(paste -sd, "$file")]"
    $PSQL -v batch="$batch" <<SQL
SELECT tierdb_upsert('public.$table'::regclass, elem) FROM jsonb_array_elements(:'batch'::jsonb) elem;
SQL
    ;;
tierdb-delete)
    [ -n "$tier_key" ] || fail "tierdb-delete needs --tier-key"
    batch="[$(paste -sd, "$file")]"
    $PSQL -tA -v batch="$batch" <<SQL
SELECT tierdb_delete('public.$table'::regclass, elem -> '$pk', (elem ->> '$tier_key')::$tier_key_type)
FROM jsonb_array_elements(:'batch'::jsonb) elem;
SQL
    ;;
stream-load)
    [ -n "$label" ] || fail "stream-load needs --label"
    curl -sS -X POST "http://localhost:9090/api/load/public.$table" \
        -H "X-TierDB-Token: $token" \
        -H "X-TierDB-Label: $label" \
        --data-binary "@$file"
    ;;
sql)
    $PSQL < "$file"
    ;;
*)
    fail "unknown mode: $mode"
    ;;
esac
