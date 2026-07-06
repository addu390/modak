#!/usr/bin/env bash
# The core TierDB feature set on the base stack, no engine overlay needed.
# A tiered table, corrections landing in the delta, a mirrored table via CDC,
# a cross-mode join, and labeled HTTP stream loads, one continuous narrative
# because each part builds on the state the previous one left behind.
set -euo pipefail
source "$(dirname "$0")/../lib.sh"
preflight

say "0. Reset: drop and unregister this scenario's tables from any previous run"
reset_table trip_events
reset_table vehicles

say "1. Create a range-partitioned hot table and load its seed dataset"
example/ingest.sh trip_events example/datasets/trip_events/schema.sql sql
example/ingest.sh trip_events example/datasets/trip_events/seed.jsonl insert
echo "   5 rows across 3 partitions, high-water event_time = 250"

say "2. Register the table with TierDB (creates the cold Iceberg table)"
docker compose run --rm worker register --table public.trip_events --pk id --tier-key event_time

say "3. Worker tiers everything behind the high-water mark"
wait_for "cut-line advanced to 200 (p0+p1 tiered)" \
    "SELECT tier_key_hi FROM tierdb.cutline WHERE table_id = 'public.trip_events'::regclass::oid::bigint" \
    "200"
wait_for "tiered partitions physically dropped" \
    "SELECT count(*) FROM pg_inherits JOIN pg_class c ON c.oid = inhrelid \
     WHERE inhparent = 'public.trip_events'::regclass \
     AND c.relname IN ('trip_events_p0', 'trip_events_p1')" \
    "0"

echo "   plain SELECT still sees ALL rows (transparent two-tier read):"
$PSQL -c "SELECT * FROM public.trip_events ORDER BY id"
echo "   ... while the raw heap holds only the recent partition:"
$PSQL -c "SET tierdb.transparent_reads = off; SELECT * FROM public.trip_events ORDER BY id"

assert_eq "transparent read sees all rows" "5" \
    "$($PSQL -tA -c 'SELECT count(*) FROM public.trip_events')"
assert_eq "raw heap holds only the hot partition" "1" \
    "$($PSQL -tA -c 'SET tierdb.transparent_reads = off; SELECT count(*) FROM public.trip_events')"

say "4. Route corrections: tombstone id=1, correct id=3 (both cold), insert id=6 (hot)"
example/ingest.sh trip_events example/datasets/trip_events/correction-deletes.jsonl tierdb-delete --tier-key event_time
example/ingest.sh trip_events example/datasets/trip_events/correction-upsert-cold.jsonl tierdb-upsert
example/ingest.sh trip_events example/datasets/trip_events/upsert-new-hot.jsonl tierdb-upsert
$PSQL -c "SELECT pk, op, tier_key FROM tierdb.delta ORDER BY pk"

say "5. Compaction folds the delta into Iceberg (equality deletes)"
wait_for "delta folded and cleared" "SELECT count(*) FROM tierdb.delta" "0"

EXPECTED="2|20|b
3|110|C!
4|150|d
5|250|e
6|260|f"

say "6. One pinned read spanning both tiers (explicit protocol)"
RESULT=$($PSQL -tA <<'SQL'
BEGIN;
SET LOCAL duckdb.max_workers_per_postgres_scan = 0;
SELECT pin_id AS pin FROM tierdb_read_begin('public.trip_events'::regclass) \gset
SELECT tierdb_rewrite_scan('public.trip_events'::regclass) AS scan_sql \gset
SELECT string_agg(id::text || '|' || event_time::text || '|' || coalesce(val,''), E'\n' ORDER BY id)
FROM ( :scan_sql ) q;
SELECT tierdb_read_end(:pin) \gset _
COMMIT;
SQL
)
echo "$RESULT"
assert_eq "pinned read (explicit protocol)" "$EXPECTED" "$RESULT"

say "6b. The same read as ONE plain SQL statement (planner hook)"
RESULT_TRANSPARENT=$($PSQL -tA -c \
    "SELECT string_agg(id::text || '|' || event_time::text || '|' || coalesce(val,''), E'\n' ORDER BY id) FROM public.trip_events")
echo "$RESULT_TRANSPARENT"
assert_eq "transparent read" "$EXPECTED" "$RESULT_TRANSPARENT"

say "6c. Plain UPDATE and DELETE on cold rows (planner rewrite, real lake scan)"
EXPECTED="2|20|B?
3|110|C!
5|250|e
6|260|f"
example/ingest.sh trip_events example/datasets/trip_events/dml-updates.jsonl update
example/ingest.sh trip_events example/datasets/trip_events/dml-deletes.jsonl delete
RESULT_DML=$($PSQL -tA -c \
    "SELECT string_agg(id::text || '|' || event_time::text || '|' || coalesce(val,''), E'\n' ORDER BY id) FROM public.trip_events")
assert_eq "transparent read reflects the cold DML immediately" "$EXPECTED" "$RESULT_DML"

say "6d. Compaction folds the cold DML into Iceberg"
wait_for "cold DML folded and cleared" "SELECT count(*) FROM tierdb.delta" "0"
RESULT_FOLDED=$($PSQL -tA -c \
    "SELECT string_agg(id::text || '|' || event_time::text || '|' || coalesce(val,''), E'\n' ORDER BY id) FROM public.trip_events")
assert_eq "read after fold matches" "$EXPECTED" "$RESULT_FOLDED"

say "7. Create an ordinary (unpartitioned) table and load its seed dataset"
example/ingest.sh vehicles example/datasets/vehicles/schema.sql sql
example/ingest.sh vehicles example/datasets/vehicles/seed.jsonl insert
echo "   3 vehicles, these predate registration and travel via the initial copy"

say "8. Register it MIRRORED (publication + slot + initial copy to Iceberg)"
docker compose run --rm worker register \
    --table public.vehicles --pk id --tier-key last_seen --mode mirrored

say "9. Plain DML, no TierDB API involved, and the mirror trails it"
example/ingest.sh vehicles example/datasets/vehicles/dml-inserts.jsonl insert
example/ingest.sh vehicles example/datasets/vehicles/dml-updates.jsonl update
example/ingest.sh vehicles example/datasets/vehicles/dml-deletes.jsonl delete
TARGET_LSN=$($PSQL -tA -c "SELECT (pg_current_wal_insert_lsn() - '0/0'::pg_lsn)::bigint")
wait_for "mirror frontier caught up past the DML (lsn $TARGET_LSN)" \
    "SELECT replicated_lsn >= $TARGET_LSN
       FROM tierdb.cutline WHERE table_id = 'public.vehicles'::regclass::oid::bigint" \
    "t"

EXPECTED="1|VIN-001|active|100
2|VIN-002|repair|210
4|VIN-004|active|260"

say "10. Same rows from the heap and from Iceberg (hybrid read)"
echo "   default read, the heap alone, it is complete:"
HEAP=$($PSQL -tA -c \
    "SELECT string_agg(id::text || '|' || vin || '|' || status || '|' || last_seen::text, E'\n' ORDER BY id) FROM public.vehicles")
echo "$HEAP"
assert_eq "mirrored heap read" "$EXPECTED" "$HEAP"

echo "   hybrid read, the same query served from the Iceberg mirror:"
HYBRID=$($PSQL -tA <<'SQL'
SET tierdb.mirrored_reads = 'hybrid';
SET duckdb.max_workers_per_postgres_scan = 0;
SELECT string_agg(id::text || '|' || vin || '|' || status || '|' || last_seen::text, E'\n' ORDER BY id) FROM public.vehicles;
SQL
)
echo "$HYBRID"
assert_eq "mirrored hybrid read" "$EXPECTED" "$HYBRID"

say "11. Cross-mode join: tiered trip events (two tiers) x mirrored vehicles"
JOIN=$($PSQL -tA -c \
    "SELECT string_agg(e.id::text || '|' || coalesce(e.val,'') || '|' || v.vin, E'\n' ORDER BY e.id)
       FROM public.trip_events e JOIN public.vehicles v ON v.id = e.id")
echo "$JOIN"
assert_eq "cross-mode join" "2|B?|VIN-002" "$JOIN"

say "12. A hot-only labeled batch lands in the heap via COPY + upsert"
R1=$(example/ingest.sh trip_events example/datasets/trip_events/stream-hot.jsonl stream-load --label sl-hot)
echo "   $R1"
echo "$R1" | grep -q '"hot_rows":2' || fail "expected 2 hot rows: $R1"
echo "$R1" | grep -q '"replay":false' || fail "expected a fresh apply: $R1"
assert_eq "hot rows visible immediately" "2" \
    "$($PSQL -tA -c "SET tierdb.transparent_reads = off;
        SELECT count(*) FROM public.trip_events WHERE id IN (7, 8)")"

say "13. A straddling batch: one row to the heap, one below the cut-line to the delta"
R2=$(example/ingest.sh trip_events example/datasets/trip_events/stream-straddle.jsonl stream-load --label sl-straddle)
echo "   $R2"
echo "$R2" | grep -q '"hot_rows":1' || fail "expected 1 hot row: $R2"
echo "$R2" | grep -q '"delta_rows":1' || fail "expected 1 delta row: $R2"

say "14. Replaying a finished label returns the recorded result and applies nothing"
R3=$(example/ingest.sh trip_events example/datasets/trip_events/stream-replay.jsonl stream-load --label sl-hot)
echo "   $R3"
echo "$R3" | grep -q '"replay":true' || fail "expected a replay: $R3"
assert_eq "the replayed batch left no trace" "0" \
    "$($PSQL -tA -c 'SELECT count(*) FROM public.trip_events WHERE id = 99')"

say "15. The wrong token is rejected"
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "http://localhost:9090/api/load/public.trip_events" \
    -H "X-TierDB-Token: wrong" -H "X-TierDB-Label: sl-nope" \
    --data-binary '{"id":1,"event_time":265,"val":"x"}')
assert_eq "401 without the right token" "401" "$CODE"

say "16. After the sweep folds the delta, one plain SELECT sees every loaded row"
wait_for "delta folded and cleared" "SELECT count(*) FROM tierdb.delta" "0"
EXPECTED="2|20|B?
3|110|C!
5|250|e
6|260|f
7|265|g
8|270|h
9|280|i
10|130|j"
RESULT=$($PSQL -tA -c \
    "SELECT string_agg(id::text || '|' || event_time::text || '|' || coalesce(val,''), E'\n' ORDER BY id) FROM public.trip_events")
echo "$RESULT"
assert_eq "stream-loaded rows across both tiers" "$EXPECTED" "$RESULT"

echo ""
echo "CORE SCENARIO PASS: tiered read, delta corrections, mirrored CDC, cross-mode join, stream load."
