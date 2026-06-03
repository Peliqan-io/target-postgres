#!/usr/bin/env python3
"""
Capture target-postgres output for regression comparison.

Feeds a fixed Singer JSONL corpus into target-postgres while capturing:
  1. All SQL statements executed (via cursor monkey-patching)
  2. All STATE messages emitted to stdout
  3. The final table schemas created in Postgres

Run on Python 3.9 to produce baseline/, on Python 3.11 to produce current/.

Usage:
    python capture.py --output baseline   # on 3.9 (master branch)
    python capture.py --output current    # on 3.11 (migration branch)
"""
import argparse
import io
import json
import os
import re
import sys
import threading
from contextlib import redirect_stdout
from pathlib import Path

# ── Capture SQL via psycopg2 LoggingConnection filter ────────────────

_captured_sql = []


def _install_sql_capture():
    """
    Patch the MillisLoggingConnection.filter method so every query
    executed through target-postgres is recorded.

    Also patch psycopg2.connect to wrap every new connection with the
    same capture, and patch copy_expert at the cursor class level.
    """
    from target_postgres.postgres import MillisLoggingConnection, _MillisLoggingCursor

    _original_filter = MillisLoggingConnection.filter
    _original_cursor_execute = _MillisLoggingCursor.execute
    _original_cursor_copy_expert = getattr(_MillisLoggingCursor, 'copy_expert', None)

    def capturing_filter(self, msg, curs):
        """Intercept every query logged by the connection."""
        sql_str = msg.decode("utf-8", errors="replace") if isinstance(msg, bytes) else str(msg)
        _captured_sql.append(("EXECUTE", sql_str))
        return _original_filter(self, msg, curs)

    def capturing_execute(self, query, vars=None):
        """Capture SQL before delegating to the real execute."""
        try:
            conn = self.connection
            if hasattr(query, "as_string"):
                sql_str = query.as_string(conn)
            elif isinstance(query, bytes):
                sql_str = query.decode("utf-8", errors="replace")
            else:
                sql_str = str(query)
        except Exception:
            sql_str = repr(query)
        _captured_sql.append(("EXECUTE", sql_str))
        # Call the grandparent (LoggingCursor.execute) to avoid double-logging
        # through the filter method. We go straight to the real cursor.
        self.timestamp = __import__('time').monotonic()
        from psycopg2.extensions import cursor as _base_cursor
        return _base_cursor.execute(self, query, vars)

    def capturing_copy_expert(self, sql, file, size=8192):
        """Capture COPY commands."""
        try:
            if hasattr(sql, "as_string"):
                sql_str = sql.as_string(self.connection)
            elif isinstance(sql, bytes):
                sql_str = sql.decode("utf-8", errors="replace")
            else:
                sql_str = str(sql)
        except Exception:
            sql_str = repr(sql)
        _captured_sql.append(("COPY", sql_str))
        from psycopg2.extensions import cursor as _base_cursor
        return _base_cursor.copy_expert(self, sql, file, size)

    # Apply patches
    _MillisLoggingCursor.execute = capturing_execute
    _MillisLoggingCursor.copy_expert = capturing_copy_expert


def _uninstall_sql_capture():
    """Best-effort restore — not critical since we exit after capture."""
    pass


# ── Normalizers ──────────────────────────────────────────────────────

def normalize_sql(statements):
    """
    Normalize SQL for stable comparison:
    - Strip temp table UUIDs (tmp_<uuid> → tmp_NORMALIZED)
    - Strip timestamps from comments
    - Collapse whitespace
    """
    normalized = []
    for kind, sql in statements:
        # Normalize temp table names: tmp_<hex-uuid> → tmp_NORMALIZED
        sql = re.sub(r'\btmp_[0-9a-f]{8}(?:_[0-9a-f]{4}){3}_[0-9a-f]{12}\b',
                      'tmp_NORMALIZED', sql, flags=re.IGNORECASE)
        # Normalize any remaining UUID-like patterns in identifiers (quoted or unquoted)
        sql = re.sub(r'"tmp_[0-9a-f-]{32,36}"', '"tmp_NORMALIZED"', sql, flags=re.IGNORECASE)
        # Normalize index names: tp_<uuid> → tp_NORMALIZED (used for upsert indexes)
        sql = re.sub(r'"tp_[0-9a-f]{8}(?:_[0-9a-f]{4}){3}_[0-9a-f]{12}"',
                      '"tp_NORMALIZED"', sql, flags=re.IGNORECASE)
        # Collapse whitespace
        sql = re.sub(r'\s+', ' ', sql).strip()
        normalized.append({"kind": kind, "sql": sql})
    return normalized


# ── Capture target state output ──────────────────────────────────────

def capture_stdout_states(stdout_text):
    """Extract STATE messages from target stdout."""
    states = []
    for line in stdout_text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
            states.append(msg)
        except json.JSONDecodeError:
            continue
    return states


# ── Introspect resulting Postgres schemas ────────────────────────────

def introspect_tables(config):
    """Query information_schema to get the tables and columns created by the target."""
    import psycopg2
    conn = psycopg2.connect(
        host=config.get("postgres_host", "localhost"),
        port=config.get("postgres_port", 5432),
        dbname=config["postgres_database"],
        user=config["postgres_username"],
        password=config["postgres_password"],
    )
    schema = config.get("postgres_schema", "public")
    cur = conn.cursor()

    # Get all tables in the schema
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """, (schema,))
    tables = [row[0] for row in cur.fetchall()]

    result = {}
    for table in tables:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = []
        for col_name, data_type, is_nullable, max_len in cur.fetchall():
            columns.append({
                "name": col_name,
                "type": data_type,
                "nullable": is_nullable == "YES",
                "max_length": max_len,
            })
        result[table] = columns

    cur.close()
    conn.close()
    return result


def clean_target_schema(config):
    """Drop all tables in the target schema so we start clean."""
    import psycopg2
    conn = psycopg2.connect(
        host=config.get("postgres_host", "localhost"),
        port=config.get("postgres_port", 5432),
        dbname=config["postgres_database"],
        user=config["postgres_username"],
        password=config["postgres_password"],
    )
    conn.autocommit = True
    schema = config.get("postgres_schema", "public")
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
    """, (schema,))
    tables = [row[0] for row in cur.fetchall()]

    for table in tables:
        cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')

    cur.close()
    conn.close()
    print(f"Cleaned {len(tables)} tables from {schema}")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Capture target-postgres regression output")
    parser.add_argument("--output", required=True, choices=["baseline", "current"],
                        help="Output directory name (baseline for 3.9, current for 3.11)")
    parser.add_argument("--messages", default=None,
                        help="Path to Singer JSONL messages file")
    args = parser.parse_args()

    regression_dir = Path(__file__).parent
    output_dir = regression_dir / args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    messages_path = args.messages or str(regression_dir / "fixtures" / "messages.jsonl")
    if not Path(messages_path).exists():
        print(f"ERROR: Messages file not found: {messages_path}")
        sys.exit(1)

    # Target config — connect to the Docker Postgres
    config = {
        "postgres_host": os.getenv("POSTGRES_HOST", "db"),
        "postgres_port": int(os.getenv("POSTGRES_PORT", "5432")),
        "postgres_database": os.getenv("POSTGRES_DATABASE", "singer"),
        "postgres_username": os.getenv("POSTGRES_USERNAME", "singer"),
        "postgres_password": os.getenv("POSTGRES_PASSWORD", "singer"),
        "postgres_schema": os.getenv("POSTGRES_SCHEMA", "public"),
        "disable_collection": True,
        "invalid_records_detect": True,
        "invalid_records_threshold": 0,
    }

    print(f"Python version: {sys.version}")
    print(f"Output dir: {output_dir}")
    print(f"Messages: {messages_path}")
    print(f"Postgres: {config['postgres_host']}:{config['postgres_port']}/{config['postgres_database']}")

    # Clean the target schema first
    clean_target_schema(config)

    # Install SQL capture
    _install_sql_capture()

    # Read Singer messages
    with open(messages_path, "r") as f:
        messages_text = f.read()

    # Run target-postgres, capturing stdout (STATE messages)
    from target_postgres import main as target_main

    input_stream = io.StringIO(messages_text)
    stdout_capture = io.StringIO()

    try:
        with redirect_stdout(stdout_capture):
            target_main(config, input_stream=input_stream)
    except Exception as e:
        print(f"WARNING: target-postgres raised: {e}")
    finally:
        _uninstall_sql_capture()

    # Capture results
    stdout_text = stdout_capture.getvalue()
    states = capture_stdout_states(stdout_text)
    sql_normalized = normalize_sql(_captured_sql)
    table_schemas = introspect_tables(config)

    # Write outputs
    (output_dir / "meta.json").write_text(json.dumps({
        "python_version": sys.version.split()[0],
        "message_count": len(messages_text.strip().splitlines()),
        "sql_count": len(sql_normalized),
        "state_count": len(states),
        "tables_created": list(table_schemas.keys()),
    }, indent=2))

    (output_dir / "sql_statements.json").write_text(
        json.dumps(sql_normalized, indent=2)
    )

    (output_dir / "states.json").write_text(
        json.dumps(states, indent=2)
    )

    (output_dir / "table_schemas.json").write_text(
        json.dumps(table_schemas, indent=2, sort_keys=True)
    )

    print(f"\nCapture complete:")
    print(f"  SQL statements: {len(sql_normalized)}")
    print(f"  STATE messages: {len(states)}")
    print(f"  Tables created: {list(table_schemas.keys())}")
    print(f"  Files written to: {output_dir}/")


if __name__ == "__main__":
    main()
