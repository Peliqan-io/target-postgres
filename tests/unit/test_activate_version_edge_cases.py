"""
Rigorous, real-DB edge cases for the EXACT-SET SWAP in postgres.activate_version().

These go beyond the two obvious failure modes in
test_activate_version_exact_set_swap.py (long-name reswap leak, orphan mis-swap)
and probe every branch of the rewritten method plus its interaction with the
rest of the target:

  - names that change under canonicalization (uppercase / special chars)
  - deeply / multiply nested subtables
  - repeated version bumps (no orphan accumulation)
  - version guards (older version ignored, same version re-activated)
  - the empty-response synthesis branch (and its version guard)
  - leftover orphan staging tables (this stream, other streams, many versions)
  - metadata integrity: missing comment, non-JSON comment on a co-existing table
  - a subtable that is brand new in the new version (the live_exists=False branch)
  - dependent views, primary keys/indexes, and _sdc_table_version after the swap

Deliberately OUT OF SCOPE: a *live* (clean) table name >= 63 chars -- that is
Postgres's hard identifier limit on the destination table and no staging trick
can fix it.
"""
from copy import deepcopy
import json

import psycopg2
from psycopg2 import sql

from utils.fixtures import CatStream, NestedStream, FakeStream, CONFIG, db_cleanup, TEST_DB
from target_postgres import main


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _tables():
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='public'")
        return {r[0] for r in cur.fetchall()}


def _count(table):
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT count(*) FROM public.{}").format(
            sql.Identifier(table)))
        return cur.fetchone()[0]


def _leaks():
    return {t for t in _tables() if t.startswith('pqtemp__')}


def _version(table):
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT obj_description(%s::regclass)", ('public.' + table,))
        row = cur.fetchone()
        if not row or not row[0]:
            return None
        return json.loads(row[0]).get('version')


def _named_cat(name, n, version, nested_count=3):
    s = CatStream(n, version=version, nested_count=nested_count)
    s.stream = name
    s.schema = deepcopy(s.schema)
    s.schema['stream'] = name
    return s


class RecordsStream(FakeStream):
    """A stream that emits an explicit schema + fixed record list, so nested
    arrays can be turned on/off precisely across versions."""

    def __init__(self, stream_name, properties, records, version=None,
                 key_properties=('id',)):
        self._recs = list(records)
        super().__init__(len(records), version=version)
        self.stream = stream_name
        self.schema = {
            'type': 'SCHEMA',
            'stream': stream_name,
            'schema': {'additionalProperties': False, 'properties': properties},
            'key_properties': list(key_properties),
        }

    def generate_record(self):
        return deepcopy(self._recs[self.id - 1])


ARRAY_PROPS = {
    'id': {'type': ['integer']},
    'tags': {'type': ['null', 'array'], 'items': {'type': ['integer']}},
}


# ---------------------------------------------------------------------------
# 1. Names that change under canonicalization  (the bug this suite found)
# ---------------------------------------------------------------------------

def test_canonicalizing_stream_name_swaps_correctly(db_cleanup):
    """`C@ts` -> physical `c_ts`: write_batch records the staging/live path from
    the RAW stream name, so the swap must match on the raw name, not the
    canonicalized one. Otherwise the swap silently no-ops and the live table
    keeps stale data."""
    main(CONFIG, input_stream=_named_cat('C@ts', 100, version=1))
    main(CONFIG, input_stream=_named_cat('C@ts', 60, version=2))
    assert _count('c_ts') == 60
    assert _version('c_ts') == 2
    assert not _leaks()


def test_uppercase_stream_name_swaps_correctly(db_cleanup):
    main(CONFIG, input_stream=_named_cat('Orders', 40, version=1))
    main(CONFIG, input_stream=_named_cat('Orders', 25, version=2))
    assert _count('orders') == 25
    assert _version('orders') == 2
    assert not _leaks()


# ---------------------------------------------------------------------------
# 2. Nesting shapes
# ---------------------------------------------------------------------------

def test_deeply_nested_stream_reswaps_every_subtable(db_cleanup):
    """NestedStream has arrays-of-arrays and deep object->array paths, so a swap
    touches many subtables. Every one must swap (version advances, no leak)."""
    main(CONFIG, input_stream=NestedStream(20, version=1))
    root_and_subs = {t for t in _tables() if t == 'root' or t.startswith('root__')}
    assert len(root_and_subs) > 3, f"expected several nested tables, got {root_and_subs}"

    main(CONFIG, input_stream=NestedStream(12, version=2))
    assert not _leaks()
    # every table belonging to this stream advanced to version 2
    for t in {t for t in _tables() if t == 'root' or t.startswith('root__')}:
        assert _version(t) == 2, f"{t} did not advance to v2"
    assert _count('root') == 12


def test_nested_subtable_rows_correct_after_swap(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1, nested_count=3))
    main(CONFIG, input_stream=CatStream(40, version=2, nested_count=2))
    assert _count('cats') == 40
    # 40 cats * 2 immunizations = 80 (not the v1 300); i.e. no stale rows remain
    assert _count('cats__adoption__immunizations') == 80
    assert not _leaks()


# ---------------------------------------------------------------------------
# 3. Repeated version bumps -- no orphan accumulation
# ---------------------------------------------------------------------------

def test_three_version_bumps_no_orphan_accumulation(db_cleanup):
    for ver, rows in [(1, 100), (2, 70), (3, 55), (4, 33)]:
        main(CONFIG, input_stream=CatStream(rows, version=ver, nested_count=2))
        assert _count('cats') == rows, f"v{ver} count wrong"
        assert not _leaks(), f"staging tables leaked after v{ver}: {_leaks()}"
    assert _version('cats') == 4


# ---------------------------------------------------------------------------
# 4. Version guards
# ---------------------------------------------------------------------------

def test_older_version_is_ignored_and_preserves_data(db_cleanup):
    """A stale/older ACTIVATE_VERSION must not clobber a newer live table."""
    main(CONFIG, input_stream=CatStream(100, version=5))
    main(CONFIG, input_stream=CatStream(30, version=2))  # older -> ignored
    assert _count('cats') == 100
    assert _version('cats') == 5
    assert not _leaks()


def test_reactivate_same_version_is_noop(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=3))
    main(CONFIG, input_stream=CatStream(45, version=3))  # same version
    # already-active guard: no swap; the live table is left as the first load
    assert _count('cats') == 100
    assert _version('cats') == 3
    assert not _leaks()


# ---------------------------------------------------------------------------
# 5. Empty-response synthesis branch (+ its version guard)
# ---------------------------------------------------------------------------

def test_empty_response_clears_root_and_nested(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1, nested_count=3))
    assert _count('cats') == 100 and _count('cats__adoption__immunizations') > 0
    main(CONFIG, input_stream=CatStream(0, version=2))  # empty full-table
    assert _count('cats') == 0
    assert _count('cats__adoption__immunizations') == 0
    assert _version('cats') == 2
    assert not _leaks()


def test_empty_response_on_versionless_table_does_not_wipe(db_cleanup):
    """INCREMENTAL first (no version -> lands in live table, version null). Then a
    FULL_TABLE run whose staging is empty must NOT be treated as an empty
    response and wipe freshly-loaded data (the synthesis branch is guarded on
    the table already having an active version)."""
    main(CONFIG, input_stream=CatStream(100))            # no version
    assert _version('cats') is None
    # A versioned run whose records also went straight to the live table
    # (version-less -> versioned transition) must keep its rows.
    main(CONFIG, input_stream=CatStream(50, version=1))
    assert _count('cats') > 0


# ---------------------------------------------------------------------------
# 6. Leftover orphan staging tables
# ---------------------------------------------------------------------------

def _forge_orphan(path_root, sentinel=999):
    """Create a staging table that looks like a leftover from an aborted run:
    a realistic path comment and one sentinel row."""
    name = path_root[:63]
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE TABLE public.{} (id bigint)").format(
                sql.Identifier(name)))
            cur.execute(sql.SQL("INSERT INTO public.{} VALUES (%s)").format(
                sql.Identifier(name)), (sentinel,))
            cur.execute(sql.SQL("COMMENT ON TABLE public.{} IS %s").format(
                sql.Identifier(name)),
                (json.dumps({'path': [path_root], 'version': 3,
                             'schema_version': 2}),))
    return name


def test_multiple_orphans_same_stream_ignored(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    o1 = _forge_orphan('pqtemp__cats__3')
    o2 = _forge_orphan('pqtemp__cats__7')
    main(CONFIG, input_stream=CatStream(60, version=10))
    assert _count('cats') == 60          # real v10 swapped in, not an orphan
    assert _count(o1) == 1 and _count(o2) == 1   # orphans untouched
    assert _version('cats') == 10


def test_orphan_for_other_stream_ignored(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    other = _forge_orphan('pqtemp__dogs__4')
    main(CONFIG, input_stream=CatStream(50, version=2))
    assert _count('cats') == 50
    assert _count(other) == 1            # a different stream's orphan is untouched


# ---------------------------------------------------------------------------
# 7. Metadata integrity
# ---------------------------------------------------------------------------

def test_staging_table_without_comment_is_ignored(db_cleanup):
    """A pqtemp__ table with NO path comment is not in the cache, so it is never
    swapped -- a safe no-op, not a crash. The real swap still happens."""
    main(CONFIG, input_stream=CatStream(100, version=1))
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE public.pqtemp__cats__999 (id bigint)")
            cur.execute("INSERT INTO public.pqtemp__cats__999 VALUES (1)")
    main(CONFIG, input_stream=CatStream(40, version=2))
    assert _count('cats') == 40
    assert _count('pqtemp__cats__999') == 1   # untouched, ignored


def test_coexisting_table_with_plaintext_comment_does_not_break(db_cleanup):
    """A user table in the same schema with a NON-JSON comment must not break
    activation (setup_table_mapping_cache reads every comment in the schema)."""
    main(CONFIG, input_stream=CatStream(100, version=1))
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE public.human_notes (id bigint)")
            cur.execute("COMMENT ON TABLE public.human_notes IS "
                        "'just some human notes, not JSON'")
    main(CONFIG, input_stream=CatStream(30, version=2))
    assert _count('cats') == 30
    assert 'human_notes' in _tables()
    assert not _leaks()


# ---------------------------------------------------------------------------
# 8. Brand-new subtable in the new version (the live_exists=False branch)
# ---------------------------------------------------------------------------

def test_new_subtable_appearing_in_new_version_is_promoted(db_cleanup):
    """v1's SCHEMA has no array -> no subtable exists. v2 introduces the `tags`
    array -> the subtable is brand new (no live counterpart). activate_version
    must promote the staging subtable via a single rename (live_exists=False)."""
    id_only = {'id': {'type': ['integer']}}
    v1 = RecordsStream('widgets', id_only,
                       [{'id': i} for i in range(1, 6)], version=1)
    main(CONFIG, input_stream=v1)
    assert 'widgets' in _tables()
    assert 'widgets__tags' not in _tables()          # no subtable yet

    v2 = RecordsStream('widgets', ARRAY_PROPS,
                       [{'id': i, 'tags': [1, 2, 3]} for i in range(1, 6)],
                       version=2)
    main(CONFIG, input_stream=v2)
    assert _count('widgets') == 5
    assert 'widgets__tags' in _tables()              # promoted
    assert _count('widgets__tags') == 15             # 5 * 3
    assert not _leaks()


# ---------------------------------------------------------------------------
# 9. Interaction with dependent objects, keys, and version column
# ---------------------------------------------------------------------------

def test_dependent_view_is_preserved_across_swap(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE VIEW cats_ids AS SELECT id FROM public.cats")
    main(CONFIG, input_stream=CatStream(50, version=2))
    # the view was dropped and recreated around the rename, and now sees v2 data
    assert 'cats_ids' in _view_names()
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.cats_ids")
        assert cur.fetchone()[0] == 50
    # drop the view so db_cleanup's DROP TABLE (no CASCADE) can drop cats
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS public.cats_ids")


def _view_names():
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.views "
                    "WHERE table_schema='public'")
        return {r[0] for r in cur.fetchall()}


def test_primary_key_and_index_preserved_across_swap(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    main(CONFIG, input_stream=CatStream(50, version=2))
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT count(*) FROM information_schema.table_constraints
            WHERE table_schema='public' AND table_name='cats'
              AND constraint_type='PRIMARY KEY'
        """)
        assert cur.fetchone()[0] == 1, "primary key lost after swap"
        cur.execute("SELECT count(*) FROM pg_indexes "
                    "WHERE schemaname='public' AND tablename='cats'")
        assert cur.fetchone()[0] >= 1, "indexes lost after swap"


def test_sdc_table_version_reflects_new_version_after_swap(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    main(CONFIG, input_stream=CatStream(50, version=2))
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.cats WHERE _sdc_table_version = 2")
        assert cur.fetchone()[0] == 50
        cur.execute("SELECT count(*) FROM public.cats WHERE _sdc_table_version = 1")
        assert cur.fetchone()[0] == 0, "stale v1 rows survived the swap"
