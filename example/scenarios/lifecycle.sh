#!/usr/bin/env bash
# Lifecycle: kill an initial copy mid-flight, resume it, verify, unregister.
set -euo pipefail
source "$(dirname "$0")/../lib.sh"
preflight

say "0. Reset: drop and unregister this scenario's table from any previous run"
docker rm -f tierdb-example-copy >/dev/null 2>&1 || true
reset_table tire_pressure_logs

say "1. A bigger mirrored table, and an initial copy we kill mid-flight"
example/ingest.sh tire_pressure_logs example/datasets/tire_pressure_logs/schema.sql sql
example/ingest.sh tire_pressure_logs example/datasets/tire_pressure_logs/seed.sql sql
echo "   20000 rows, registering with --chunk-rows 500, then killing the copy"
docker compose run -d --name tierdb-example-copy worker register \
    --table public.tire_pressure_logs --pk id --tier-key ts --mode mirrored --chunk-rows 500 >/dev/null
COPY_SEEN=0
for _ in $(seq 1 240); do
    CHUNKS=$($PSQL -tA -c "SELECT coalesce(max(chunks_done), -1) FROM tierdb.copy_progress
        WHERE table_id = 'public.tire_pressure_logs'::regclass::oid::bigint")
    if [ "$CHUNKS" -ge 2 ]; then COPY_SEEN=1; break; fi
    sleep 0.5
done
if [ "$COPY_SEEN" != "1" ]; then
    docker logs tierdb-example-copy >&2 || true
    fail "timeout waiting for the initial copy to journal progress"
fi
docker kill tierdb-example-copy >/dev/null
docker rm tierdb-example-copy >/dev/null
KILLED_AT=$($PSQL -tA -c "SELECT chunks_done FROM tierdb.copy_progress
    WHERE table_id = 'public.tire_pressure_logs'::regclass::oid::bigint")
echo "   killed after $KILLED_AT chunk(s), the journal row survives and the slot pins WAL"
[ -n "$KILLED_AT" ] || fail "copy_progress row missing after kill"

say "2. Re-running the same register resumes from the journal"
docker compose run --rm worker register \
    --table public.tire_pressure_logs --pk id --tier-key ts --mode mirrored --chunk-rows 500
wait_for "copy finished (journal row cleared, frontier seeded)" \
    "SELECT (SELECT count(*) FROM tierdb.copy_progress WHERE table_id = 'public.tire_pressure_logs'::regclass::oid::bigint) = 0
        AND (SELECT replicated_lsn IS NOT NULL FROM tierdb.cutline WHERE table_id = 'public.tire_pressure_logs'::regclass::oid::bigint)" \
    "t"

say "3. verify: heap vs lake must match exactly (exits non-zero on mismatch)"
docker compose run --rm worker verify --table public.tire_pressure_logs

say "4. unregister: catalog rows, slot, and publication all gone"
docker compose run --rm worker unregister --table public.tire_pressure_logs --drop-lake
LEFTOVERS=$($PSQL -tA -c "
    SELECT (SELECT count(*) FROM tierdb.tables WHERE table_name = 'tire_pressure_logs')
         + (SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'tierdb_slot_public_tire_pressure_logs')
         + (SELECT count(*) FROM pg_publication WHERE pubname = 'tierdb_pub_public_tire_pressure_logs')")
assert_eq "unregister leaves nothing behind" "0" "$LEFTOVERS"

echo ""
echo "LIFECYCLE SCENARIO PASS: resumable initial copy, journal survives a kill, verify, clean unregister."
