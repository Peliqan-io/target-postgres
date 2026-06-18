"""
Unit tests for PostgresTarget.delete_records().

These exercise the SQL-building, batching and best-effort error handling of the
DELETERECORD delete path without a live Postgres: the cursor/connection and the
table/schema lookups are faked, so the test runs anywhere (the live-DB suite in
test_postgres.py covers end-to-end behavior).
"""
import pytest

from target_postgres.postgres import PostgresTarget


class FakeCursor:
    def __init__(self, rowcounts=None, raise_on_delete=False):
        # list of (stmt, params); BEGIN; is a plain string, DELETE is a Composable
        self.executed = []
        self._rowcounts = list(rowcounts or [])
        self.rowcount = 0
        self.raise_on_delete = raise_on_delete

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        if isinstance(stmt, str):  # e.g. 'BEGIN;'
            self.executed.append((stmt, params))
            return
        # Composed DELETE statement
        if self.raise_on_delete:
            raise Exception("simulated execute failure")
        self.executed.append((stmt, params))
        self.rowcount = self._rowcounts.pop(0) if self._rowcounts else 0

    def delete_params(self):
        return [params for (stmt, params) in self.executed if not isinstance(stmt, str)]


class FakeConn:
    def __init__(self, cur):
        self._cur = cur
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def _make_target(cur, schema_props=("ID",)):
    """Build a PostgresTarget without running __init__ (which would connect to PG)."""
    target = PostgresTarget.__new__(PostgresTarget)
    target.conn = FakeConn(cur)
    target.postgres_schema = "public"
    target.table_mapping_cache = {}
    # Stub the DB-touching helpers so the test needs no live Postgres.
    target.setup_table_mapping_cache = lambda c: None
    target.add_table_mapping_helper = lambda path, cache: {"to": path[0]}
    if schema_props is None:
        target.get_table_schema = lambda c, name: None
    else:
        target.get_table_schema = lambda c, name: {
            "schema": {"properties": {col: {} for col in schema_props}}
        }
    return target


# WHY: happy path — a single batch deletes by PK, commits, returns rowcount.
def test_deletes_by_single_pk_and_commits():
    cur = FakeCursor(rowcounts=[2])
    target = _make_target(cur, schema_props=("ID",))

    deleted = target.delete_records("Accounts", [{"ID": "a1"}, {"ID": "a2"}])

    assert deleted == 2
    assert target.conn.committed is True
    assert cur.delete_params() == [["a1", "a2"]]


# WHY: composite PKs must flatten params as (v1a, v2a, v1b, v2b, ...) matching
#      the WHERE (col1, col2) IN (...) tuple ordering.
def test_deletes_by_composite_pk():
    cur = FakeCursor(rowcounts=[1])
    target = _make_target(cur, schema_props=("ID", "_sdc_division"))

    deleted = target.delete_records(
        "Journals", [{"ID": "g1", "_sdc_division": 100}]
    )

    assert deleted == 1
    assert cur.delete_params() == [["g1", 100]]


# WHY: large delete sets are split into DELETE_BATCH_SIZE-sized statements.
def test_batches_deletes():
    cur = FakeCursor(rowcounts=[2, 2, 1])
    target = _make_target(cur, schema_props=("ID",))
    target.DELETE_BATCH_SIZE = 2

    records = [{"ID": f"a{i}"} for i in range(5)]
    deleted = target.delete_records("Accounts", records)

    params = cur.delete_params()
    assert len(params) == 3                  # 2 + 2 + 1
    assert params[0] == ["a0", "a1"]
    assert params[2] == ["a4"]
    assert deleted == 5


# WHY: empty input is a no-op (and must not touch the connection).
def test_no_records_is_noop():
    cur = FakeCursor()
    target = _make_target(cur)

    assert target.delete_records("Accounts", []) == 0
    assert cur.executed == []
    assert target.conn.committed is False


# WHY: missing target table -> skip + rollback, never raise.
def test_missing_table_is_skipped():
    cur = FakeCursor()
    target = _make_target(cur, schema_props=None)  # get_table_schema -> None

    deleted = target.delete_records("Ghost", [{"ID": "x"}])

    assert deleted == 0
    assert cur.delete_params() == []           # no DELETE issued
    assert target.conn.rolled_back is True


# WHY: a PK column absent from the table -> skip + rollback, never raise.
def test_missing_pk_column_is_skipped():
    cur = FakeCursor()
    target = _make_target(cur, schema_props=("OTHER",))

    deleted = target.delete_records("Accounts", [{"ID": "x"}])

    assert deleted == 0
    assert cur.delete_params() == []
    assert target.conn.rolled_back is True


# WHY: a failure executing the DELETE is best-effort — rolled back, returns 0,
#      and must NOT propagate (per the ticket: deletes never error the run).
def test_execute_failure_is_swallowed():
    cur = FakeCursor(raise_on_delete=True)
    target = _make_target(cur, schema_props=("ID",))

    deleted = target.delete_records("Accounts", [{"ID": "x"}])

    assert deleted == 0
    assert target.conn.rolled_back is True
