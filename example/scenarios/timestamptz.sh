#!/usr/bin/env bash
# A TIERED table keyed on timestamptz: the codec, day layout, and native SQL.
set -euo pipefail
source "$(dirname "$0")/../lib.sh"
preflight

say "0. Reset: drop and unregister this scenario's table from any previous run"
reset_table gps_pings

say "1. Create a daily-partitioned timestamptz table and load its seed dataset"
example/ingest.sh gps_pings example/datasets/gps_pings/schema.sql sql
example/ingest.sh gps_pings example/datasets/gps_pings/seed.jsonl insert
echo "   5 rows across 3 daily partitions, high-water ts = 2026-01-03 08:00+00"

say "2. Register with the timestamp column as the tier key (type is detected)"
docker compose run --rm worker register --table public.gps_pings --pk id --tier-key ts

say "3. Worker tiers the days behind the high-water mark"
wait_for "cut-line advanced to 2026-01-03 (d1+d2 tiered)" \
    "SELECT tier_key_hi = (extract(epoch FROM timestamptz '2026-01-03 00:00:00+00') * 1000000)::bigint \
     FROM tierdb.cutline WHERE table_id = 'public.gps_pings'::regclass::oid::bigint" \
    "t"
wait_for "tiered partitions physically dropped" \
    "SELECT count(*) FROM pg_inherits JOIN pg_class c ON c.oid = inhrelid \
     WHERE inhparent = 'public.gps_pings'::regclass \
     AND c.relname IN ('gps_pings_d1', 'gps_pings_d2')" \
    "0"

echo "   plain SELECT with a native timestamptz predicate spans both tiers:"
$PSQL -c "SELECT * FROM public.gps_pings WHERE ts < '2026-01-03 00:00:00+00' ORDER BY id"

assert_eq "transparent read sees all rows" "5" \
    "$($PSQL -tA -c 'SELECT count(*) FROM public.gps_pings')"
assert_eq "native predicate on cold rows answers from the lake" "4" \
    "$($PSQL -tA -c "SELECT count(*) FROM public.gps_pings WHERE ts < '2026-01-03 00:00:00+00'")"
assert_eq "raw heap holds only the hot day" "1" \
    "$($PSQL -tA -c 'SET tierdb.transparent_reads = off; SELECT count(*) FROM public.gps_pings')"

say "4. A native-typed delete routes by the encoded key"
routed=$(example/ingest.sh gps_pings example/datasets/gps_pings/tierdb-deletes.jsonl tierdb-delete \
    --tier-key ts --tier-key-type timestamptz)
echo "   tierdb_delete routed to: $routed"
assert_eq "cold delete became a delta tombstone" "delta" "$routed"
assert_eq "transparent read hides the tombstoned row" "4" \
    "$($PSQL -tA -c 'SELECT count(*) FROM public.gps_pings')"

echo ""
echo "TIMESTAMPTZ SCENARIO PASS: daily tiering, native predicates, native-typed correction."
