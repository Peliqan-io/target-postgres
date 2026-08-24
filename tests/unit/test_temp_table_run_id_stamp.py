"""Temp tables carry the pipeline run id in their table comment (PQ-3846).

The Peliqan backend sets PELIQAN_PIPELINE_RUN_ID on this process. Every pqtemp__
table gets stamped with it so the backend can later drop exactly the temp tables
belonging to runs that have finished, instead of guessing from table names.

Postgres has a single metadata slot per table (COMMENT ON TABLE -> pg_description)
and it already holds the Singer mappings, so the id has to ride inside the same
JSON envelope -- writing it separately would clobber them. _set_table_metadata is
the one place this file writes that comment, so stamping there covers every table.

No database needed: the stamp is decided before the statement is executed, so a
fake cursor is enough to read back the JSON that would have been written.
"""
import json
from unittest.mock import MagicMock

import pytest
from psycopg2 import sql

from target_postgres.postgres import PostgresTarget
from target_postgres.sql_base import (
    PIPELINE_RUN_ID_ENV_VAR,
    PQ_RUN_ID_KEY,
    TEMP_TABLE_MARKER,
)


def write_comment(monkeypatch, table_name, metadata, run_id="4242", commit=True):
    """Call _set_table_metadata and return the JSON it put in the comment."""
    if run_id is None:
        monkeypatch.delenv(PIPELINE_RUN_ID_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(PIPELINE_RUN_ID_ENV_VAR, run_id)

    target = PostgresTarget.__new__(PostgresTarget)
    target.postgres_schema = "public"

    cur = MagicMock()
    target._set_table_metadata(cur, table_name, metadata, commit=commit)

    composed = cur.execute.call_args.args[0]
    literals = [part.wrapped for part in composed.seq if isinstance(part, sql.Literal)]
    assert len(literals) == 1, "expected exactly one literal: the metadata JSON"
    return json.loads(literals[0])


def test_temp_table_is_stamped_with_the_run_id(monkeypatch):
    written = write_comment(monkeypatch, TEMP_TABLE_MARKER + "cats__1", {})

    assert written[PQ_RUN_ID_KEY] == "4242"


def test_the_stamp_rides_alongside_the_mappings(monkeypatch):
    """One merged comment: a second write would clobber whatever the first put there."""
    mappings = {"id": {"type": "integer"}}

    written = write_comment(
        monkeypatch,
        TEMP_TABLE_MARKER + "cats__1",
        {"mappings": mappings, "version": 3, "schema_version": 2},
    )

    assert written["mappings"] == mappings
    assert written["version"] == 3
    assert written["schema_version"] == 2
    assert written[PQ_RUN_ID_KEY] == "4242"


def test_live_tables_are_not_stamped(monkeypatch):
    written = write_comment(monkeypatch, "cats", {"mappings": {}})

    assert PQ_RUN_ID_KEY not in written


def test_a_live_table_is_unstamped_when_it_inherits_a_stamp(monkeypatch):
    """activate_version renames pqtemp__<stream>__<v> onto the live name.

    Postgres carries the comment across ALTER TABLE ... RENAME, so the published
    table arrives holding the staging table's run id. Writing the live name back
    must strip it, or a live table keeps a stale stamp forever.
    """
    written = write_comment(
        monkeypatch, "cats", {"mappings": {}, PQ_RUN_ID_KEY: "999"}
    )

    assert PQ_RUN_ID_KEY not in written


def test_no_stamp_without_the_env_var(monkeypatch):
    """The target also runs outside Peliqan; an absent id must not break it."""
    written = write_comment(
        monkeypatch, TEMP_TABLE_MARKER + "cats__1", {"mappings": {}}, run_id=None
    )

    assert PQ_RUN_ID_KEY not in written
    assert written["mappings"] == {}


def test_the_callers_metadata_dict_is_not_mutated(monkeypatch):
    """Callers reuse the dict they pass in; the stamp is ours, not theirs."""
    metadata = {"mappings": {}}

    write_comment(monkeypatch, TEMP_TABLE_MARKER + "cats__1", metadata)

    assert metadata == {"mappings": {}}


@pytest.mark.parametrize(
    "table_name",
    [TEMP_TABLE_MARKER + "cats__1", TEMP_TABLE_MARKER + "a" * 55],
)
def test_the_prefix_survives_identifier_truncation(monkeypatch, table_name):
    """Names are truncated to 63 chars from the tail, so the prefix always stays."""
    assert len(table_name) <= PostgresTarget.IDENTIFIER_FIELD_LENGTH

    written = write_comment(monkeypatch, table_name, {})

    assert written[PQ_RUN_ID_KEY] == "4242"


def test_the_batch_table_stamp_does_not_commit(monkeypatch):
    """write_table_batch stamps between record batches.

    _set_table_metadata commits by default, which schema setup relies on. Doing it
    there would commit the preceding batches' INSERT/UPDATEs into the live table,
    turning a crashed load from all-or-nothing into partially published.
    """
    monkeypatch.setenv(PIPELINE_RUN_ID_ENV_VAR, "4242")

    target = PostgresTarget.__new__(PostgresTarget)
    target.postgres_schema = "public"
    cur = MagicMock()

    target._set_table_metadata(cur, TEMP_TABLE_MARKER + "abc", {}, commit=False)
    assert cur.connection.commit.call_count == 0

    target._set_table_metadata(cur, TEMP_TABLE_MARKER + "abc", {})
    assert cur.connection.commit.call_count == 1
