#!/usr/bin/env bash
# Runs every scenario end to end against the docker compose stack.
# Prereq: make -C example up finished and the worker is running.
# Each scenario resets its own tables, seeds them, and asserts its own
# results, so they can also be run individually: make -C example scenario-core.
set -euo pipefail
cd "$(dirname "$0")"

./scenarios/core.sh
./scenarios/lifecycle.sh
./scenarios/timestamptz.sh
./scenarios/trino.sh
./scenarios/spark.sh

echo ""
echo "EXAMPLE PASS:"
echo "  core:        tiered read, delta corrections, mirrored CDC, cross-mode join, stream load."
echo "  lifecycle:   a copy killed mid-flight resumed from the journal, verify matched"
echo "               heap and lake exactly, and unregister left nothing behind."
echo "  timestamptz: a timestamp-keyed table tiered by day, read and corrected with"
echo "               native timestamptz SQL, no epoch columns anywhere."
echo "  trino:       a real Trino query spanned hot+cold tiers through the modak catalog,"
echo "               corrections applied via the delta overlay (set EXAMPLE_TRINO=1)."
echo "  spark:       ModakSpark.read spanned hot+cold tiers with no SQL layer at all"
echo "               (set EXAMPLE_SPARK=1)."
