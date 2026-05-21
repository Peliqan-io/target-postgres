#!/usr/bin/env bash
# Run capture on Python 3.11 (python-311-migration branch).
#
# Prerequisites:
#   docker compose -f tests/regression/docker-compose.regression.yml up -d db
#   Baseline already captured via run_capture_39.sh
#
# Usage (from target-postgres root):
#   bash tests/regression/run_capture_311.sh

set -euo pipefail

TARGET_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
REGRESSION_DIR="$TARGET_DIR/tests/regression"

# Detect docker compose network name
NETWORK=$(docker network ls --format '{{.Name}}' | grep -i regression | head -1)
if [ -z "$NETWORK" ]; then
  echo "ERROR: No regression network found. Run docker compose up first."
  exit 1
fi

echo "==> Capturing current output on Python 3.11 (migration branch)"
echo "    Target dir: $TARGET_DIR"
echo "    Network:    $NETWORK"

CURRENT_BRANCH=$(cd "$TARGET_DIR" && git branch --show-current)
echo "    Git branch: $CURRENT_BRANCH"

if [ "$CURRENT_BRANCH" != "python-311-migration" ]; then
  echo ""
  echo "WARNING: You are on '$CURRENT_BRANCH', not 'python-311-migration'."
  echo "For the 3.11 capture, you should be on python-311-migration. Continuing anyway..."
  echo ""
fi

docker run --rm \
  --network "$NETWORK" \
  -v "$TARGET_DIR":/target \
  -w /target \
  -e POSTGRES_HOST=db \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_DATABASE=singer \
  -e POSTGRES_USERNAME=singer \
  -e POSTGRES_PASSWORD=singer \
  -e POSTGRES_SCHEMA=public \
  python:3.11-slim \
  bash -c '
    apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1
    pip install --upgrade pip -q 2>/dev/null
    pip install poetry -q 2>/dev/null
    cd /target
    poetry config virtualenvs.create false
    poetry install --no-interaction -q 2>/dev/null
    python tests/regression/capture.py --output current
  '

echo ""
echo "==> Current output captured in tests/regression/current/"
ls -la "$REGRESSION_DIR/current/"
