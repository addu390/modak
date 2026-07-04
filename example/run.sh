#!/usr/bin/env bash
# Runs the full example end to end against the docker compose stack.
# Prereq: `docker compose up -d --build` finished and the worker is running.
# Each step asserts its own results and fails fast.
set -euo pipefail
cd "$(dirname "$0")"

./steps/00-reset.sh
./steps/01-tiering.sh
./steps/02-corrections.sh
./steps/03-mirroring.sh
./steps/04-lifecycle.sh
./steps/05-stream-load.sh

echo ""
echo "EXAMPLE PASS:"
echo "  tiered:    old partitions moved to Iceberg; corrections folded by compaction;"
echo "             hot+cold merged exactly once — explicit protocol AND one plain SELECT."
echo "  mirrored:  plain INSERT/UPDATE/DELETE reached Iceberg via CDC; the hybrid read"
echo "             served the same rows from the lake; the cross-mode join just worked."
echo "  lifecycle: a copy killed mid-flight resumed from the journal, verify matched"
echo "             heap and lake exactly, and unregister left nothing behind."
echo "  stream:    labeled HTTP micro-batches routed per row across both tiers, and a"
echo "             replayed label returned its recorded result without applying anything."
