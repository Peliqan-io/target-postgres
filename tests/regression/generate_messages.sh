#!/usr/bin/env bash
# Generate Singer messages from tap-google-sheets using Python 3.9.
# The output is saved to fixtures/messages.jsonl
#
# Usage: bash tests/regression/generate_messages.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TAP_DIR="$(cd "$SCRIPT_DIR/../../../tap-google-sheets" && pwd)"
FIXTURES_DIR="$SCRIPT_DIR/fixtures"

echo "==> Generating Singer messages from tap-google-sheets in python:3.9-slim"
echo "    Tap dir: $TAP_DIR"
echo "    Output:  $FIXTURES_DIR/messages.jsonl"

docker run --rm \
  -v "$TAP_DIR":/tap \
  -v "$FIXTURES_DIR":/output \
  -w /tap \
  -e AES_SECRET_KEY="peliqan-test-key" \
  -e TAP_GOOGLE_SHEETS_CLIENT_ID="${TAP_GOOGLE_SHEETS_CLIENT_ID:?}" \
  -e TAP_GOOGLE_SHEETS_CLIENT_SECRET="${TAP_GOOGLE_SHEETS_CLIENT_SECRET:?}" \
  -e TAP_GOOGLE_SHEETS_REFRESH_TOKEN="${TAP_GOOGLE_SHEETS_REFRESH_TOKEN:?}" \
  -e TAP_GOOGLE_SHEETS_SPREADSHEET_ID="${TAP_GOOGLE_SHEETS_SPREADSHEET_ID:?}" \
  -e TAP_GOOGLE_SHEETS_START_DATE="${TAP_GOOGLE_SHEETS_START_DATE:-2010-01-01T00:00:00Z}" \
  python:3.9-slim \
  bash -c '
    apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1
    python -m venv /venv
    /venv/bin/pip install --upgrade pip -q 2>/dev/null
    /venv/bin/pip install -e . -q 2>/dev/null

    # Write config from env vars
    cat > /tmp/config.json <<EOF
{
  "client_id": "${TAP_GOOGLE_SHEETS_CLIENT_ID:?}",
  "client_secret": "${TAP_GOOGLE_SHEETS_CLIENT_SECRET:?}",
  "refresh_token": "${TAP_GOOGLE_SHEETS_REFRESH_TOKEN:?}",
  "spreadsheet_id": "${TAP_GOOGLE_SHEETS_SPREADSHEET_ID:?}",
  "start_date": "${TAP_GOOGLE_SHEETS_START_DATE:-2010-01-01T00:00:00Z}",
  "user_agent": "tap-google-sheets (dev@peliqan.io)",
  "sheets_selected": "",
  "request_timeout": 300,
  "disable_collection": true
}
EOF

    echo "--- Discovering catalog ---" >&2
    /venv/bin/tap-google-sheets --config /tmp/config.json --discover > /tmp/catalog.json 2>/dev/null

    # Select all streams
    /venv/bin/python -c "
import json
with open(\"/tmp/catalog.json\") as f:
    catalog = json.load(f)
for stream in catalog.get(\"streams\", []):
    for entry in stream.get(\"metadata\", []):
        entry.setdefault(\"metadata\", {})[\"selected\"] = True
with open(\"/tmp/catalog_selected.json\", \"w\") as f:
    json.dump(catalog, f)
print(f\"Selected {len(catalog.get('streams', []))} streams\")
"

    echo "--- Running sync ---" >&2
    /venv/bin/tap-google-sheets \
      --config /tmp/config.json \
      --catalog /tmp/catalog_selected.json \
      > /output/messages.jsonl 2>/dev/null

    LINES=$(wc -l < /output/messages.jsonl)
    echo "--- Generated $LINES Singer message lines ---" >&2
  '

echo ""
echo "==> Done. Messages saved to $FIXTURES_DIR/messages.jsonl"
wc -l "$FIXTURES_DIR/messages.jsonl"
