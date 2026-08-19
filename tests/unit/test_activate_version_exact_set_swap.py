"""
Regression tests for the EXACT-SET SWAP in postgres.activate_version().

Background
----------
FULL_TABLE replication writes each new version into marked staging tables
(pqtemp__<stream>__<version>[...]) and activate_version() renames them onto the
clean live names. The old implementation discovered the staging tables with a
truncation-lossy scan:

    SELECT tablename FROM pg_tables
    WHERE tablename LIKE '<pqtemp__stream__version>[:63]%'

then rebuilt the live name by slicing the physical string
(root_table_name + tail). For a long stream name the 63-char search prefix drops
the version suffix entirely, so the scan also matches staging tables *left over
from an earlier/aborted run* and mis-pairs them -- corrupting the swap or
crashing in _get_table_metadata().

The exact-set swap pairs staging -> live by the *paths* recorded in
table_mapping_cache (the source of truth for every physical name, truncation and
collision-suffix already applied), and never slices a physical name. It only
ever touches this run's staging tables.

These tests assert the CORRECT post-swap state. Run them against the fix (pass)
and against the pre-fix code (fail) to demonstrate the regression is closed.
"""
from copy import deepcopy

import psycopg2
from psycopg2 import sql

from utils.fixtures import CatStream, CONFIG, db_cleanup, TEST_DB
from target_postgres import main


def _long_stream(name, n, version, nested_count=3):
    """A CatStream whose stream name is `name` (used to force staging-name
    overflow past Postgres' 63-char identifier limit)."""
    stream = CatStream(n, version=version, nested_count=nested_count)
    stream.stream = name
    stream.schema = deepcopy(stream.schema)
    stream.schema['stream'] = name
    return stream


def _tables_in_public(cur):
    cur.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public'"
    )
    return {r[0] for r in cur.fetchall()}


def _count(cur, table_name):
    cur.execute(sql.SQL('SELECT count(*) FROM public.{}').format(
        sql.Identifier(table_name)))
    return cur.fetchone()[0]


# A 57-char stream name: pqtemp__<57 chars>__<version> overflows 63, so the
# staging *physical* name is truncated and the old LIKE prefix loses the version.
LONG_NAME = 'a_very_long_stream_name_that_exceeds_the_limit_padding_xx'
assert len(LONG_NAME) == 57 and len('pqtemp__' + LONG_NAME + '__1') > 63


def test_full_table_reswap_long_name_with_subtable(db_cleanup):
    """The everyday path: two FULL_TABLE versions of a long-named stream that has
    a nested subtable. After the second activation the live tables must hold
    exactly the second batch and no pqtemp__ staging table may leak."""
    main(CONFIG, input_stream=_long_stream(LONG_NAME, 100, version=1))
    main(CONFIG, input_stream=_long_stream(LONG_NAME, 80, version=10))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            tables = _tables_in_public(cur)

            # No staging table may survive activation.
            leaked = {t for t in tables if t.startswith('pqtemp__')}
            assert not leaked, f'leaked staging tables: {leaked}'

            # The live root is exactly the canonical stream name (57 chars, fits);
            # the nested subtable is <root>__<truncated>. Detect them by exact name /
            # prefix -- NOT by substring, since truncation cuts "__adoption..." off.
            root = LONG_NAME
            assert root in tables, tables
            assert _count(cur, root) == 80

            subtables = [t for t in tables if t != root and t.startswith(root + '__')]
            assert len(subtables) == 1, subtables
            assert _count(cur, subtables[0]) > 0


def test_orphan_staging_table_is_ignored_by_activate_version(db_cleanup):
    """A staging table left over from an earlier aborted run (a *different*
    version) shares the truncated LIKE prefix of the current version. The old
    code's LIKE scan matched it and mis-paired it against the live root; the
    exact-set swap ignores it (its path root != this run's versioned root) and
    still activates the current version correctly."""
    # v1 establishes the live tables.
    main(CONFIG, input_stream=_long_stream(LONG_NAME, 100, version=1))

    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            live = LONG_NAME
            assert live in _tables_in_public(cur)

            # Forge an orphan staging table from a simulated aborted version-3
            # run: its untruncated path root is pqtemp__<stream>__3, which the
            # truncated LIKE prefix of ANY later version also matches. Give it a
            # realistic path comment (as write_batch would) and a sentinel row.
            orphan_path_root = 'pqtemp__' + LONG_NAME + '__3'
            orphan_name = orphan_path_root[:63]
            cur.execute(sql.SQL(
                'CREATE TABLE public.{} (id bigint)').format(
                sql.Identifier(orphan_name)))
            cur.execute(sql.SQL('INSERT INTO public.{} VALUES (999)').format(
                sql.Identifier(orphan_name)))
            cur.execute(sql.SQL('COMMENT ON TABLE public.{} IS {}').format(
                sql.Identifier(orphan_name),
                sql.Literal('{"path": ["%s"], "version": 3, "schema_version": 2}'
                            % orphan_path_root)))

    # v10 must activate cleanly despite the orphan sharing the LIKE prefix.
    main(CONFIG, input_stream=_long_stream(LONG_NAME, 80, version=10))

    with psycopg2.connect(**TEST_DB) as conn:
        with conn.cursor() as cur:
            tables = _tables_in_public(cur)
            live = LONG_NAME
            assert live in tables

            # The live root holds the v10 batch -- NOT the orphan's sentinel row,
            # and not a mangled count from swapping the wrong table.
            assert _count(cur, live) == 80

            # The orphan was never touched by the swap (cleanup is a separate
            # task's job); its sentinel row is intact.
            assert orphan_name in tables
            assert _count(cur, orphan_name) == 1
