#!/bin/bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

echo "=== Parser tests ==="
bats "$SCRIPT_DIR/test_parser.bats"

echo ""
echo "=== Visualization tests ==="
if command -v node > /dev/null 2>&1; then
    node "$SCRIPT_DIR/test_visualization.js"
else
    echo "SKIP: node not found — install Node.js to run visualization tests"
fi

echo ""
echo "All tests passed."
