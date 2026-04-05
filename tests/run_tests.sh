#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

echo "=== sstable_timeline tests ==="
bash "$SCRIPT_DIR/sstable_timeline/run_tests.sh"

echo ""
echo "=== sstablemetadata_viz tests ==="
bash "$SCRIPT_DIR/sstablemetadata_viz/run_tests.sh"

echo ""
echo "All tests passed."
