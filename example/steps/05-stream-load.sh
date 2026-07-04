#!/usr/bin/env bash
# Stream Load: labeled micro-batches over HTTP, routed per row, and a replayed
# label proving exactly-once.
set -euo pipefail
source "$(dirname "$0")/../lib.sh"

LOAD_URL="http://localhost:9090/api/load/public.events"
TOKEN="modak-example"

load() { # label jsonl
    curl -sS -X POST "$LOAD_URL" \
        -H "X-Modak-Token: $TOKEN" \
        -H "X-Modak-Label: $1" \
        --data-binary "$2"
}

say "16. A hot-only labeled batch lands in the heap via COPY + upsert"
R1=$(load sl-hot $'{"id":7,"event_time":265,"val":"g"}\n{"id":8,"event_time":270,"val":"h"}')
echo "   $R1"
echo "$R1" | grep -q '"hot_rows":2' || fail "expected 2 hot rows: $R1"
echo "$R1" | grep -q '"replay":false' || fail "expected a fresh apply: $R1"
assert_eq "hot rows visible immediately" "2" \
    "$($PSQL -tA -c "SET modak.transparent_reads = off;
        SELECT count(*) FROM public.events WHERE id IN (7, 8)")"

say "17. A straddling batch: one row to the heap, one below the cut-line to the delta"
R2=$(load sl-straddle $'{"id":9,"event_time":280,"val":"i"}\n{"id":10,"event_time":130,"val":"j"}')
echo "   $R2"
echo "$R2" | grep -q '"hot_rows":1' || fail "expected 1 hot row: $R2"
echo "$R2" | grep -q '"delta_rows":1' || fail "expected 1 delta row: $R2"

say "18. Replaying a finished label returns the recorded result and applies nothing"
R3=$(load sl-hot $'{"id":99,"event_time":265,"val":"NOT ME"}')
echo "   $R3"
echo "$R3" | grep -q '"replay":true' || fail "expected a replay: $R3"
assert_eq "the replayed batch left no trace" "0" \
    "$($PSQL -tA -c 'SELECT count(*) FROM public.events WHERE id = 99')"

say "19. The wrong token is rejected"
CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$LOAD_URL" \
    -H "X-Modak-Token: wrong" -H "X-Modak-Label: sl-nope" \
    --data-binary '{"id":1,"event_time":265,"val":"x"}')
assert_eq "401 without the right token" "401" "$CODE"

say "20. After the sweep folds the delta, one plain SELECT sees every loaded row"
wait_for "delta folded and cleared" "SELECT count(*) FROM modak.delta" "0"
EXPECTED="2|20|B?
3|110|C!
5|250|e
6|260|f
7|265|g
8|270|h
9|280|i
10|130|j"
RESULT=$($PSQL -tA -c \
    "SELECT string_agg(id::text || '|' || event_time::text || '|' || coalesce(val,''), E'\n' ORDER BY id) FROM public.events")
echo "$RESULT"
assert_eq "stream-loaded rows across both tiers" "$EXPECTED" "$RESULT"
