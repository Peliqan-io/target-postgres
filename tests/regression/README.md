# target-postgres — Python 3.9 → 3.11 Regression Tests

Before/after parity test for the Python 3.9 → 3.11 migration.

## Strategy

1. **Generate** a fixed Singer JSONL corpus from tap-google-sheets (done once, committed).
2. **Capture baseline** on `master` branch + Python 3.9: feed messages through target-postgres, capture all SQL statements, STATE output, and resulting table schemas.
3. **Capture current** on `python-311-migration` branch + Python 3.11: same process.
4. **Compare** — assert SQL, STATE, and schemas are identical.

## What's Compared

| Artifact | File | What it catches |
| --- | --- | --- |
| SQL statements | `sql_statements.json` | Type mapping, CREATE/ALTER TABLE, upsert logic, COPY commands |
| STATE output | `states.json` | Bookmark tracking, watermark logic |
| Table schemas | `table_schemas.json` | Column names, types, nullability — the end result |
| Meta | `meta.json` | Python version, table list, counts |

## Files

| File | Purpose |
| --- | --- |
| `fixtures/messages.jsonl` | Singer messages from tap-google-sheets (fixed input corpus) |
| `capture.py` | Runs target-postgres with SQL capture, writes output to baseline/ or current/ |
| `test_regression.py` | pytest suite comparing baseline/ vs current/ |
| `conftest.py` | Registers regression marker |
| `docker-compose.regression.yml` | Postgres 14 for test runs |
| `run_capture_39.sh` | Docker wrapper: Python 3.9 + master branch → baseline/ |
| `run_capture_311.sh` | Docker wrapper: Python 3.11 + migration branch → current/ |
| `run_tests.sh` | Runs the comparison tests (no Docker needed) |
| `generate_messages.sh` | Re-generates fixtures/messages.jsonl from tap-google-sheets |

## Usage

```bash
cd target-postgres

# 1. Start Postgres
docker compose -f tests/regression/docker-compose.regression.yml up -d db

# 2. Switch to master, capture baseline on Python 3.9
git checkout master
bash tests/regression/run_capture_39.sh

# 3. Switch to migration branch, capture on Python 3.11
git checkout python-311-migration
bash tests/regression/run_capture_311.sh

# 4. Compare
bash tests/regression/run_tests.sh

# 5. Cleanup
docker compose -f tests/regression/docker-compose.regression.yml down -v
```

## Notes

- The `baseline/` directory is committed to git. The `current/` directory is gitignored.
- SQL is normalized: temp table UUIDs are replaced with `tmp_NORMALIZED`, whitespace is collapsed.
- The SQL capture works by monkey-patching `psycopg2.extensions.cursor.execute` and `.copy_expert` — no changes to target-postgres source code.
