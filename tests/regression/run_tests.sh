#!/usr/bin/env bash
# Run the regression comparison tests.
#
# Prerequisites:
#   - Baseline captured via run_capture_39.sh
#   - Current captured via run_capture_311.sh
#
# Usage (from target-postgres root):
#   bash tests/regression/run_tests.sh

set -euo pipefail

TARGET_DIR="$(cd "$(dirname "$0")/../.." && pwd)"

echo "==> Running regression comparison tests"

# These tests are pure file comparisons — no DB or Docker needed
cd "$TARGET_DIR"
python3 -m pytest tests/regression/test_regression.py -v --tb=short

echo ""
echo "==> All regression tests passed!"
