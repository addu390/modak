#!/usr/bin/env bash
# Trino reads the consistent two-tier view through the tierdb catalog.
# Requires the trino overlay: EXAMPLE_TRINO=1 and make -C example add-trino.
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

if [ "${EXAMPLE_TRINO:-0}" != "1" ]; then
    say "skipping Trino scenario (set EXAMPLE_TRINO=1 and run: make -C example add-trino)"
    exit 0
fi
preflight

TRINO="docker compose exec -T trino trino --output-format=CSV_UNQUOTED --catalog=tierdb --schema=public"

say "0. Reset: drop and unregister this scenario's table from any previous run"
reset_table trip_events

say "1. Create a tiered table, load its seed dataset, and register it"
example/ingest.sh trip_events example/datasets/trip_events/schema.sql sql
example/ingest.sh trip_events example/datasets/trip_events/seed.jsonl insert
docker compose run --rm worker register --table public.trip_events --pk id --tier-key event_time

say "2. Worker tiers everything behind the high-water mark"
wait_for "cut-line advanced to 200 (p0+p1 tiered)" \
    "SELECT tier_key_hi FROM tierdb.cutline WHERE table_id = 'public.trip_events'::regclass::oid::bigint" \
    "200"

say "3. Wait for Trino to come up"
for _ in $(seq 1 60); do
    if $TRINO --execute "SELECT 1" >/dev/null 2>&1; then break; fi
    sleep 2
done

say "4. One Trino query spans both tiers"
$TRINO --execute "SELECT * FROM trip_events ORDER BY id"
assert_eq "Trino sees all rows across both tiers" "5" \
    "$($TRINO --execute 'SELECT count(*) FROM trip_events')"
assert_eq "raw heap holds only the hot partition" "1" \
    "$($PSQL -tA -c 'SET tierdb.transparent_reads = off; SELECT count(*) FROM public.trip_events')"

say "5. Corrections to cold rows show up in Trino through the delta overlay"
example/ingest.sh trip_events example/datasets/trip_events/correction-deletes.jsonl tierdb-delete --tier-key event_time >/dev/null
example/ingest.sh trip_events example/datasets/trip_events/correction-upsert-cold.jsonl tierdb-upsert >/dev/null
assert_eq "tombstone and correction merged newest-wins" "2,20,b
3,110,C!
4,150,d
5,250,e" \
    "$($TRINO --execute 'SELECT id, event_time, val FROM trip_events ORDER BY id')"

say "6. Pins release when the query finishes"
assert_eq "no lingering read pins" "0" \
    "$($PSQL -tA -c 'SELECT count(*) FROM tierdb.read_pins')"

echo ""
echo "TRINO SCENARIO PASS: consistent two-tier reads, delta overlay, pin lifecycle."
