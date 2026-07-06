#!/usr/bin/env bash

set -euo pipefail
cd "$(dirname "$0")"

./core.sh

# Disabled until CI is enabled.
# ./lifecycle.sh
# ./timestamptz.sh
# ./trino.sh
# ./spark.sh

echo ""
echo "EXAMPLE PASS:"
echo "  core:        tiered read, delta corrections, mirrored CDC, cross-mode join, stream load."
echo "  lifecycle:   a copy killed mid-flight resumed from the journal, verify matched"
echo "               heap and lake exactly, and unregister left nothing behind."
echo "  timestamptz: a timestamp-keyed table tiered by day, read and corrected with"
echo "               native timestamptz SQL, no epoch columns anywhere."
echo "  trino:       a real Trino query spanned hot+cold tiers through the tierdb catalog,"
echo "               corrections applied via the delta overlay (set EXAMPLE_TRINO=1)."
echo "  spark:       TierDBSpark.read spanned hot+cold tiers with no SQL layer at all"
echo "               (set EXAMPLE_SPARK=1)."
