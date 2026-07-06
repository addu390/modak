#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "$0")/../lib.sh"

if [ "${EXAMPLE_SPARK:-0}" != "1" ]; then
    say "skipping Spark scenario (set EXAMPLE_SPARK=1 and run: make -C example add-spark)"
    exit 0
fi
if [ "${EXAMPLE_CATALOG:-0}" != "1" ]; then
    say "skipping Spark scenario (set EXAMPLE_CATALOG=1 and run: make -C example add-lakekeeper)"
    exit 0
fi
preflight

say "0. Reset: drop and unregister this scenario's table from any previous run"
reset_table trip_events

say "1. Create a tiered table, load its seed dataset, and register it"
example/ingest.sh trip_events example/datasets/trip_events/schema.sql sql
example/ingest.sh trip_events example/datasets/trip_events/seed.jsonl insert
docker compose run --rm worker register --table public.trip_events --pk id --tier-key event_time

say "2. Worker tiers everything behind the high-water mark"
wait_for "cut-line advanced to 200 (p0+p1 tiered)" \
    "SELECT tier_key_hi FROM modak.cutline WHERE table_id = 'public.trip_events'::regclass::oid::bigint" \
    "200"

say "3. spark-shell reads hot and cold through ModakSpark.read"
SPARK_LOG="$(mktemp)"
if ! OUTPUT="$(docker compose run --rm spark 2>"$SPARK_LOG")"; then
    cat "$SPARK_LOG" >&2
    rm -f "$SPARK_LOG"
    fail "spark-shell exited non-zero, see its output above"
fi
rm -f "$SPARK_LOG"
echo "$OUTPUT" | grep "^ROW \|^COUNT "

assert_eq "Spark sees all rows across both tiers" "COUNT 5" \
    "$(echo "$OUTPUT" | grep '^COUNT ')"

say "4. Pins release when the read finishes"
assert_eq "no lingering read pins" "0" \
    "$($PSQL -tA -c 'SELECT count(*) FROM modak.read_pins')"

echo ""
echo "SPARK SCENARIO PASS: consistent two-tier reads, no SQL layer, pin lifecycle."
