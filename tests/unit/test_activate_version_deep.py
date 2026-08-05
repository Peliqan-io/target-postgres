"""
DEEP real-DB edge cases for the exact-set version swap in activate_version().
Authored by a multi-agent deep-research pass; every case forces a distinct branch
or adversarial input. Out of scope: a live/clean table name >= 63 chars (Postgres limit).
"""
from copy import deepcopy
import json
import psycopg2
from psycopg2 import sql

from utils.fixtures import CatStream, NestedStream, FakeStream, CONFIG, db_cleanup, TEST_DB
from target_postgres import main


def _tables():
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        return {r[0] for r in cur.fetchall()}


def _count(table):
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT count(*) FROM public.{}").format(sql.Identifier(table)))
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


def _view_names():
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.views WHERE table_schema='public'")
        return {r[0] for r in cur.fetchall()}


def _forge_orphan(path_root, sentinel=999):
    name = path_root[:63]
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE TABLE public.{} (id bigint)").format(sql.Identifier(name)))
            cur.execute(sql.SQL("INSERT INTO public.{} VALUES (%s)").format(sql.Identifier(name)), (sentinel,))
            cur.execute(sql.SQL("COMMENT ON TABLE public.{} IS %s").format(sql.Identifier(name)),
                        (json.dumps({'path': [path_root], 'version': 3, 'schema_version': 2}),))
    return name


class RecordsStream(FakeStream):
    def __init__(self, stream_name, properties, records, version=None, key_properties=('id',)):
        self._recs = list(records)
        super().__init__(len(records), version=version)
        self.stream = stream_name
        self.schema = {'type': 'SCHEMA', 'stream': stream_name,
                       'schema': {'additionalProperties': False, 'properties': properties},
                       'key_properties': list(key_properties)}

    def generate_record(self):
        return deepcopy(self._recs[self.id - 1])


ARRAY_PROPS = {'id': {'type': ['integer']}, 'tags': {'type': ['null', 'array'], 'items': {'type': ['integer']}}}


def test_disappearing_subtable_left_stale_after_version_bump(db_cleanup):
    v1_props = {'id': {'type': ['integer']},
                'tags': {'type': ['null', 'array'], 'items': {'type': ['integer']}}}
    v1 = RecordsStream('gadgets', v1_props,
                       [{'id': i, 'tags': [1, 2, 3]} for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    assert _count('gadgets__tags') == 15
    assert _version('gadgets__tags') == 1

    v2_props = {'id': {'type': ['integer']}, 'name': {'type': ['string']}}
    v2 = RecordsStream('gadgets', v2_props,
                       [{'id': i, 'name': 'x'} for i in range(1, 6)],
                       version=2)
    main(CONFIG, input_stream=v2)

    # root advanced and holds only v2's rows
    assert _count('gadgets') == 5
    assert _version('gadgets') == 2
    # the subtable that vanished from v2's SCHEMA was never staged, so it was
    # never swapped or cleared: it still holds the stale v1 rows and is still
    # stamped v1 -- an orphaned, stale subtable left behind by the bump.
    assert 'gadgets__tags' in _tables()
    assert _count('gadgets__tags') == 15
    assert _version('gadgets__tags') == 1
    assert not _leaks()


def test_bump_restamps_clean_path_comment_on_promoted_tables(db_cleanup):
    def _path(table):
        with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
            cur.execute("SELECT obj_description(%s::regclass)", ('public.' + table,))
            return json.loads(cur.fetchone()[0])['path']

    main(CONFIG, input_stream=CatStream(100, version=1, nested_count=3))
    main(CONFIG, input_stream=CatStream(40, version=2, nested_count=2))

    assert _version('cats') == 2
    # the promoted (formerly pqtemp__) tables must carry the CLEAN path in their
    # comment, not the versioned staging path.
    assert _path('cats') == ['cats']
    assert _path('cats__adoption__immunizations') == ['cats', 'adoption', 'immunizations']
    for t in ('cats', 'cats__adoption__immunizations'):
        assert not any(str(p).startswith('pqtemp__') for p in _path(t))
    assert not _leaks()


def test_versionless_to_versioned_merges_and_leaves_version_none(db_cleanup):
    main(CONFIG, input_stream=CatStream(100))          # NO version -> lands in live
    assert _version('cats') is None
    assert _count('cats') == 100

    main(CONFIG, input_stream=CatStream(50, version=1))
    # v2 ids 1..50 overlap the standing 1..100 by PK -> merged in place, not
    # replaced (a version-less live table is never staged/swapped).
    assert _count('cats') == 100
    # writing into an already-existing table does not (re)stamp a version, so the
    # live table stays version-less.
    assert _version('cats') is None
    assert not _leaks()


def test_versionless_table_is_sticky_bump_does_not_clear(db_cleanup):
    main(CONFIG, input_stream=CatStream(100))                 # version-less
    main(CONFIG, input_stream=CatStream(50, version=1))
    main(CONFIG, input_stream=CatStream(30, version=2))
    # each versioned run merged straight into the version-less live table; no
    # bump ever replaced/cleared it, and it never acquired a version.
    assert _count('cats') == 100
    assert _version('cats') is None
    assert not _leaks()


def test_key_properties_set_change_aborts_clean_and_live_untouched(db_cleanup):
    props = {'id': {'type': ['integer']}, 'sku': {'type': ['string']}}
    recs = [{'id': i, 'sku': str(i)} for i in range(1, 6)]
    main(CONFIG, input_stream=RecordsStream('items', props, recs, version=1,
                                            key_properties=('id',)))
    assert _count('items') == 5
    assert _version('items') == 1

    raised = False
    try:
        main(CONFIG, input_stream=RecordsStream('items', props, recs, version=2,
                                                key_properties=('sku',)))
    except Exception:
        raised = True
    assert raised, "changing the key_properties set must abort the load"
    # the live table is left exactly as v1: no rows lost, still version 1, and no
    # staging table was created (the guard fires before any write).
    assert _count('items') == 5
    assert _version('items') == 1
    assert not _leaks()


def test_dependent_view_on_dropped_column_recreate_is_swallowed(db_cleanup):
    v1 = RecordsStream('widgets',
                       {'id': {'type': ['integer']}, 'name': {'type': ['string']}},
                       [{'id': i, 'name': 'n%d' % i} for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE VIEW public.w_names AS SELECT name FROM public.widgets")

    # v2 drops the `name` column entirely (full-table version without it).
    v2 = RecordsStream('widgets', {'id': {'type': ['integer']}},
                       [{'id': i} for i in range(1, 4)], version=2,
                       key_properties=('id',))
    # the swap drops the dependent view, renames the staging table in, then fails
    # to recreate the view (its `name` column is gone). That recreate failure is
    # caught/rolled back inside activate_version -> the load must NOT raise.
    main(CONFIG, input_stream=v2)

    assert _count('widgets') == 3
    assert _version('widgets') == 2
    # the view could not be recreated (column no longer exists) and stays dropped.
    assert 'w_names' not in _view_names()
    assert not _leaks()

    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP VIEW IF EXISTS public.w_names")


def test_foreign_stream_named_like_versioned_root_is_hijacked(db_cleanup):
    # an unrelated INCREMENTAL (version-less) stream whose literal name collides
    # with cats' v2 staging root pqtemp__cats__2.
    foreign = RecordsStream('pqtemp__cats__2', {'id': {'type': ['integer']}},
                            [{'id': i} for i in range(1, 6)])
    main(CONFIG, input_stream=foreign)
    assert _count('pqtemp__cats__2') == 5
    assert _version('pqtemp__cats__2') is None

    main(CONFIG, input_stream=RecordsStream('cats', {'id': {'type': ['integer']}},
                                            [{'id': i} for i in range(1, 101)],
                                            version=1))
    main(CONFIG, input_stream=RecordsStream('cats', {'id': {'type': ['integer']}},
                                            [{'id': i} for i in range(1, 41)],
                                            version=2))
    # cats' v2 staging path (pqtemp__cats__2) resolves to the foreign live table;
    # write_batch appends v2's records into it and activate_version swaps it onto
    # `cats`, consuming the foreign table entirely.
    assert 'pqtemp__cats__2' not in _tables()
    assert _count('cats') == 40
    # the hijacked table was version-less, so the swap does not even stamp v2.
    assert _version('cats') is None
    assert not _leaks()



def test_empty_response_hijacked_by_same_version_orphan(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    assert _count('cats') == 100
    _forge_orphan('pqtemp__cats__2')       # sentinel id=999, comment version 3

    # a genuine EMPTY full-table response: 0 records, so write_batch returns early
    # and creates NO staging of its own.
    main(CONFIG, input_stream=CatStream(0, version=2))

    # the empty response should have cleared cats to 0; instead the same-version
    # orphan is collected as staging and swapped onto `cats`.
    assert 'pqtemp__cats__2' not in _tables()
    assert _count('cats') == 1
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM public.cats WHERE id = 999")
        assert cur.fetchone()[0] == 1
    assert _version('cats') == 3


def test_mixed_case_resync_across_runs_forks_orphan_and_leaves_stale_live(db_cleanup):
    main(CONFIG, input_stream=RecordsStream('Cats', {'id': {'type': ['integer']}},
                                            [{'id': i} for i in range(1, 101)],
                                            version=1))
    main(CONFIG, input_stream=RecordsStream('cats', {'id': {'type': ['integer']}},
                                            [{'id': i} for i in range(1, 41)],
                                            version=2))
    # `Cats` -> physical `cats`. The re-synced `cats` canonicalizes to the same
    # physical name, collides in the mapping cache, and is forked into `cats__1`.
    # Because that fork is a brand-new (version-less-at-lookup) physical table,
    # the second run never replaces the first -- the stale live table survives.
    assert 'cats' in _tables() and 'cats__1' in _tables()
    assert _count('cats') == 100          # stale first run, untouched
    assert _version('cats') == 1
    assert _count('cats__1') == 40        # the forked second run
    assert _version('cats__1') == 2
    assert not _leaks()


def test_case_only_collision_two_streams_get_suffixed_names(db_cleanup):
    main(CONFIG, input_stream=_named_cat('orders', 50, version=1, nested_count=0))
    main(CONFIG, input_stream=_named_cat('Orders', 30, version=1, nested_count=0))
    # `Orders` canonicalizes to `orders`, which already exists -> suffixed `orders__1`.
    assert 'orders' in _tables() and 'orders__1' in _tables()
    assert _count('orders') == 50
    assert _count('orders__1') == 30
    assert _version('orders') == 1
    assert _version('orders__1') == 1
    assert not _leaks()


def test_reswap_of_collision_suffixed_table_pairs_by_path(db_cleanup):
    main(CONFIG, input_stream=_named_cat('orders', 50, version=1, nested_count=0))
    main(CONFIG, input_stream=_named_cat('Orders', 30, version=1, nested_count=0))
    # bump ONLY the suffixed stream: staging lands under raw path pqtemp__Orders__2
    # (physical pqtemp__orders__2) and must pair to the LIVE table by path
    # ('Orders',) -> orders__1, NOT to the case-partner `orders`.
    main(CONFIG, input_stream=_named_cat('Orders', 15, version=2, nested_count=0))
    assert _count('orders__1') == 15
    assert _version('orders__1') == 2
    # the collision partner is left completely untouched.
    assert _count('orders') == 50
    assert _version('orders') == 1
    assert not _leaks()


def test_delete_before_activate_version_is_discarded_by_swap(db_cleanup):
    # A DELETERECORD flushed BEFORE ACTIVATE_VERSION deletes from the still-live
    # v1 table, which the version swap then renames away -- so the delete is lost.
    stream_name = 'cats_delbefore'
    props = {'id': {'type': ['integer']}, 'val': {'type': ['null', 'string']}}

    class MsgStream:
        def __init__(self, messages):
            self._m = messages
            self._i = -1

        def __iter__(self):
            return self

        def __next__(self):
            self._i += 1
            if self._i < len(self._m):
                return json.dumps(self._m[self._i])
            raise StopIteration

    def schema_msg():
        return {'type': 'SCHEMA', 'stream': stream_name,
                'schema': {'additionalProperties': False, 'properties': props},
                'key_properties': ['id']}

    def record_msg(i, v):
        return {'type': 'RECORD', 'stream': stream_name,
                'record': {'id': i, 'val': 'v%d' % i}, 'version': v}

    def activate_msg(v):
        return {'type': 'ACTIVATE_VERSION', 'stream': stream_name, 'version': v}

    run1 = [schema_msg()] + [record_msg(i, 1) for i in range(1, 21)] + [activate_msg(1)]
    main(CONFIG, input_stream=MsgStream(run1))
    assert _count(stream_name) == 20

    run2 = ([schema_msg()] + [record_msg(i, 2) for i in range(1, 11)]
            + [{'type': 'DELETERECORD', 'stream': stream_name, 'record': {'id': 5}}]
            + [activate_msg(2)])
    main(CONFIG, input_stream=MsgStream(run2))

    assert _count(stream_name) == 10
    assert _version(stream_name) == 2
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL('SELECT count(*) FROM public.{} WHERE id = 5').format(
            sql.Identifier(stream_name)))
        # delete hit the old (pre-swap) table, so id=5 survives in the promoted v2 table
        assert cur.fetchone()[0] == 1
    assert not _leaks()


def test_delete_after_activate_version_is_applied_to_promoted_table(db_cleanup):
    # A DELETERECORD flushed AFTER ACTIVATE_VERSION (by the final force flush)
    # deletes from the already-promoted v2 live table, so it takes effect.
    stream_name = 'cats_delafter'
    props = {'id': {'type': ['integer']}, 'val': {'type': ['null', 'string']}}

    class MsgStream:
        def __init__(self, messages):
            self._m = messages
            self._i = -1

        def __iter__(self):
            return self

        def __next__(self):
            self._i += 1
            if self._i < len(self._m):
                return json.dumps(self._m[self._i])
            raise StopIteration

    def schema_msg():
        return {'type': 'SCHEMA', 'stream': stream_name,
                'schema': {'additionalProperties': False, 'properties': props},
                'key_properties': ['id']}

    def record_msg(i, v):
        return {'type': 'RECORD', 'stream': stream_name,
                'record': {'id': i, 'val': 'v%d' % i}, 'version': v}

    def activate_msg(v):
        return {'type': 'ACTIVATE_VERSION', 'stream': stream_name, 'version': v}

    run1 = [schema_msg()] + [record_msg(i, 1) for i in range(1, 21)] + [activate_msg(1)]
    main(CONFIG, input_stream=MsgStream(run1))
    assert _count(stream_name) == 20

    run2 = ([schema_msg()] + [record_msg(i, 2) for i in range(1, 11)]
            + [activate_msg(2)]
            + [{'type': 'DELETERECORD', 'stream': stream_name, 'record': {'id': 5}}])
    main(CONFIG, input_stream=MsgStream(run2))

    assert _count(stream_name) == 9
    assert _version(stream_name) == 2
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL('SELECT count(*) FROM public.{} WHERE id = 5').format(
            sql.Identifier(stream_name)))
        # delete landed on the promoted v2 table, so id=5 is gone
        assert cur.fetchone()[0] == 0
    assert not _leaks()


def test_one_sibling_array_disappears_other_reswaps_partial_staleness(db_cleanup):
    # v2 keeps `tags` but drops `colors` from the schema entirely. Only `tags`
    # gets a staging table, so the swap reswaps tags but leaves the colors
    # subtable behind untouched -> partial staleness (documented behavior).
    name = 'evt_partial'
    v1_props = {
        'id': {'type': ['integer']},
        'tags': {'type': ['null', 'array'], 'items': {'type': ['integer']}},
        'colors': {'type': ['null', 'array'], 'items': {'type': ['string']}},
    }
    v1 = RecordsStream(name, v1_props,
                       [{'id': i, 'tags': [1, 2], 'colors': ['r', 'g', 'b']}
                        for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    assert _count(name + '__tags') == 10
    assert _count(name + '__colors') == 15

    v2_props = {
        'id': {'type': ['integer']},
        'tags': {'type': ['null', 'array'], 'items': {'type': ['integer']}},
    }
    v2 = RecordsStream(name, v2_props,
                       [{'id': i, 'tags': [7, 8, 9]} for i in range(1, 6)],
                       version=2)
    main(CONFIG, input_stream=v2)

    assert _count(name) == 5
    assert _version(name) == 2
    # tags was reswapped to the v2 payload and stamped v2
    assert _count(name + '__tags') == 15
    assert _version(name + '__tags') == 2
    # colors vanished from the v2 schema -> no staging -> stale v1 rows remain
    assert _count(name + '__colors') == 15
    assert _version(name + '__colors') == 1
    assert not _leaks()


def test_first_sync_versioned_writes_to_live_and_stamps_version(db_cleanup):
    # A first FULL_TABLE sync (no prior table) writes straight into the clean live
    # tables; ACTIVATE_VERSION for the already-active version is a no-op. The live
    # tables must hold the data, be stamped v1, and carry the RAW-name path.
    main(CONFIG, input_stream=CatStream(100, version=1, nested_count=3))
    assert _count('cats') == 100
    assert _version('cats') == 1
    assert not _leaks()
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT obj_description('public.cats'::regclass)")
        comment = cur.fetchone()[0]
    assert json.loads(comment)['path'] == ['cats']
    # nested subtable also written to live and stamped v1 (100 cats * 3 = 300)
    assert _count('cats__adoption__immunizations') == 300
    assert _version('cats__adoption__immunizations') == 1


def test_empty_versioned_first_sync_creates_no_table(db_cleanup):
    # A first FULL_TABLE sync that emits zero RECORDs (only SCHEMA + ACTIVATE_VERSION)
    # with persist_empty_tables falsy: write_batch is a no-op on count==0, and
    # activate_version finds no table -> nothing is created.
    main(CONFIG, input_stream=CatStream(0, version=1))
    assert 'cats' not in _tables()
    assert not _leaks()


def test_kept_empty_array_clears_subtable_and_advances_version(db_cleanup):
    # The `items` array property is kept in both versions but v2's arrays are all
    # empty. The subtable is still staged from the schema, so the swap clears the
    # live subtable to zero and advances its version.
    name = 'orders_keptempty'
    props = {
        'id': {'type': ['integer']},
        'items': {'type': ['null', 'array'],
                  'items': {'type': ['object'], 'properties': {'sku': {'type': ['string']}}}},
    }
    v1 = RecordsStream(name, props,
                       [{'id': i, 'items': [{'sku': 'a'}, {'sku': 'b'}]}
                        for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    assert _count(name + '__items') == 10

    v2 = RecordsStream(name, props,
                       [{'id': i, 'items': []} for i in range(1, 6)],
                       version=2)
    main(CONFIG, input_stream=v2)
    assert _count(name) == 5
    assert _version(name) == 2
    assert _count(name + '__items') == 0
    assert _version(name + '__items') == 2
    assert not _leaks()


def test_null_array_clears_subtable_like_empty(db_cleanup):
    # A JSON `null` array behaves like an empty array: the subtable is staged from
    # the schema and the swap clears the live subtable to zero.
    name = 'orders_nullarr'
    props = {
        'id': {'type': ['integer']},
        'items': {'type': ['null', 'array'],
                  'items': {'type': ['object'], 'properties': {'sku': {'type': ['string']}}}},
    }
    v1 = RecordsStream(name, props,
                       [{'id': i, 'items': [{'sku': 'a'}]} for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    assert _count(name + '__items') == 5

    v2 = RecordsStream(name, props,
                       [{'id': i, 'items': None} for i in range(1, 6)],
                       version=2)
    main(CONFIG, input_stream=v2)
    assert _count(name) == 5
    assert _version(name) == 2
    assert _count(name + '__items') == 0
    assert _version(name + '__items') == 2
    assert not _leaks()


def test_sibling_scalar_and_object_arrays_reswap_independently(db_cleanup):
    # Two sibling arrays -- a scalar array (-> _sdc_value subtable) and an
    # object array (-> named-column subtable) -- must each reswap to the v2 payload.
    name = 'evt_siblings'
    props = {
        'id': {'type': ['integer']},
        'tags': {'type': ['null', 'array'], 'items': {'type': ['integer']}},
        'guests': {'type': ['null', 'array'],
                   'items': {'type': ['object'], 'properties': {'name': {'type': ['string']}}}},
    }
    v1 = RecordsStream(name, props,
                       [{'id': i, 'tags': [1, 2, 3], 'guests': [{'name': 'a'}, {'name': 'b'}]}
                        for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    assert _count(name + '__tags') == 15
    assert _count(name + '__guests') == 10

    v2 = RecordsStream(name, props,
                       [{'id': i, 'tags': [9], 'guests': [{'name': 'z'}]}
                        for i in range(1, 5)],
                       version=2)
    main(CONFIG, input_stream=v2)
    assert _count(name) == 4
    assert _count(name + '__tags') == 4       # 4 rows * 1 tag
    assert _count(name + '__guests') == 4     # 4 rows * 1 guest
    assert _version(name + '__tags') == 2
    assert _version(name + '__guests') == 2
    assert not _leaks()


def test_dependent_materialized_view_recreated_and_repopulated(db_cleanup):
    # A materialized view depending on the live table must be dropped and
    # recreated (WITH DATA) around the swap, reflecting the v2 rows.
    main(CONFIG, input_stream=CatStream(20, version=1))
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE MATERIALIZED VIEW public.cats_mv AS "
                        "SELECT id FROM public.cats")

    main(CONFIG, input_stream=CatStream(8, version=2))
    try:
        assert _count('cats') == 8
        with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pg_matviews "
                        "WHERE schemaname='public' AND matviewname='cats_mv'")
            assert cur.fetchone()[0] == 1, 'materialized view was not recreated'
            cur.execute("SELECT count(*) FROM public.cats_mv")
            assert cur.fetchone()[0] == 8, 'materialized view not repopulated with v2 rows'
    finally:
        with psycopg2.connect(**TEST_DB) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("DROP MATERIALIZED VIEW IF EXISTS public.cats_mv")


def test_view_on_subtable_preserved_across_swap(db_cleanup):
    # A view depending on a NESTED subtable must survive the subtable's swap and
    # see the v2 subtable rows (40 cats * 2 immunizations = 80).
    main(CONFIG, input_stream=CatStream(100, version=1, nested_count=3))
    assert _count('cats__adoption__immunizations') == 300
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE VIEW public.imm_ids AS "
                        "SELECT _sdc_source_key_id FROM public.cats__adoption__immunizations")

    main(CONFIG, input_stream=CatStream(40, version=2, nested_count=2))
    try:
        assert 'imm_ids' in _view_names()
        assert _count('cats__adoption__immunizations') == 80
        with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM public.imm_ids")
            assert cur.fetchone()[0] == 80
    finally:
        with psycopg2.connect(**TEST_DB) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("DROP VIEW IF EXISTS public.imm_ids")



def test_metadata_tail_restamps_raw_path_survives_third_version(db_cleanup):
    # A non-canonical stream name (`C@ts` -> physical `c_ts`) across three
    # versions: the metadata tail must re-stamp the RAW-name path each time.
    main(CONFIG, input_stream=_named_cat('C@ts', 100, version=1))
    main(CONFIG, input_stream=_named_cat('C@ts', 60, version=2))
    main(CONFIG, input_stream=_named_cat('C@ts', 40, version=3))

    assert _count('c_ts') == 40
    assert _version('c_ts') == 3
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT obj_description('public.c_ts'::regclass)")
        comment = cur.fetchone()[0]
    assert json.loads(comment)['path'] == ['C@ts']
    assert not _leaks()


def test_old_table_name_overflow_uses_uuid_suffix_no_leftover(db_cleanup):
    n60 = 'z' * 60
    assert len(n60) == 60 and len(n60) <= 63
    assert len(n60 + '__old') == 65 and len(n60 + '__old') > 63

    v1 = RecordsStream(n60, {'id': {'type': ['integer']}},
                       [{'id': i} for i in range(1, 6)], version=1)
    main(CONFIG, input_stream=v1)
    assert n60 in _tables()
    assert _count(n60) == 5

    v2 = RecordsStream(n60, {'id': {'type': ['integer']}},
                       [{'id': i} for i in range(1, 4)], version=2)
    main(CONFIG, input_stream=v2)

    # The 3-step swap needed an `<name>__old` intermediate (65 chars > 63); the
    # uuid-suffix branch produced a fitting throwaway name that was dropped at
    # step 3. Nothing must leak: no staging table, no `__old` remnant.
    assert _tables() == {n60}, _tables()
    assert _count(n60) == 3
    assert _version(n60) == 2
    assert not _leaks()
    assert not any(t.endswith('__old') for t in _tables())


def test_versioned_to_versionless_merges_keeps_version(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    assert _count('cats') == 100
    assert _version('cats') == 1

    # A version-less (INCREMENTAL) run writes straight into the live table as an
    # upsert -- no ACTIVATE_VERSION, no staging, no swap. ids 1..30 already exist,
    # so they merge in place: the row count and the active version are preserved.
    main(CONFIG, input_stream=CatStream(30))
    assert _count('cats') == 100, 'version-less run replaced instead of merging'
    assert _version('cats') == 1, 'active version must survive a version-less merge'
    assert not _leaks()


def test_key_properties_type_change_aborts_clean_and_live_untouched(db_cleanup):
    v1 = RecordsStream('items',
                       {'id': {'type': ['integer']}, 'sku': {'type': ['string']}},
                       [{'id': i, 'sku': str(i)} for i in range(1, 6)],
                       version=1, key_properties=('id',))
    main(CONFIG, input_stream=v1)
    assert _count('items') == 5
    assert _version('items') == 1

    # Same key set {'id'} but the pk column changes integer -> string. write_batch
    # must reject this before touching anything (guarding against a destructive
    # type rewrite of the live table).
    v2 = RecordsStream('items',
                       {'id': {'type': ['string']}, 'sku': {'type': ['string']}},
                       [{'id': str(i), 'sku': str(i)} for i in range(1, 6)],
                       version=2, key_properties=('id',))
    raised = False
    try:
        main(CONFIG, input_stream=v2)
    except Exception:
        raised = True
    assert raised, 'pk type change must abort the load'

    # Live table is fully intact: same rows, same version, id still bigint.
    assert _count('items') == 5
    assert _version('items') == 1
    assert not _leaks()
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT data_type FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name='items' "
                    "AND column_name='id'")
        assert cur.fetchone()[0] == 'bigint', 'live pk column type was mutated'


def test_duplicate_pks_within_bump_dedup_to_distinct_count(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))
    assert _count('cats') == 100

    # v2 re-emits up to 25 already-seen ids (1..50) with a higher sequence inside
    # the same bump. The staging upsert must dedup by pk, so the swapped-in live
    # table holds exactly the 50 distinct ids -- never 50 + duplicates.
    main(CONFIG, input_stream=CatStream(50, version=2, nested_count=2, duplicates=25))
    assert _count('cats') == 50, 'duplicate pks within the bump were not deduped'
    assert _version('cats') == 2
    assert not _leaks()


def test_sequence_desc_last_write_wins_within_bump(db_cleanup):
    class SeqStream(FakeStream):
        def __init__(self, stream_name, properties, record_seqs, version,
                     key_properties=('id',)):
            super().__init__(len(record_seqs), version=version)
            self.stream = stream_name
            self._record_seqs = list(record_seqs)
            self._i = 0
            self.schema = {
                'type': 'SCHEMA', 'stream': stream_name,
                'schema': {'additionalProperties': False, 'properties': properties},
                'key_properties': list(key_properties),
            }

        def __next__(self):
            if not self.wrote_schema:
                self.wrote_schema = True
                return json.dumps(self.schema)
            if self._i < len(self._record_seqs):
                rec, seq = self._record_seqs[self._i]
                self._i += 1
                return json.dumps({'type': 'RECORD', 'stream': self.stream,
                                   'record': deepcopy(rec), 'sequence': seq,
                                   'version': self.version})
            if not self.wrote_activate_version:
                self.wrote_activate_version = True
                return json.dumps({'type': 'ACTIVATE_VERSION',
                                   'stream': self.stream, 'version': self.version})
            raise StopIteration

    props = {'id': {'type': ['integer']}, 'val': {'type': ['string']}}
    v1 = SeqStream('seq_lww', props, [({'id': 1, 'val': 'seed'}, 1)], version=1)
    main(CONFIG, input_stream=v1)
    assert _count('seq_lww') == 1
    assert _version('seq_lww') == 1

    # Two records for the same pk in one bump: the lower sequence arrives first,
    # the higher sequence last. sequence-DESC dedup must keep the higher one.
    v2 = SeqStream('seq_lww', props,
                   [({'id': 1, 'val': 'low'}, 100), ({'id': 1, 'val': 'high'}, 200)],
                   version=2)
    main(CONFIG, input_stream=v2)
    assert _count('seq_lww') == 1
    assert _version('seq_lww') == 2
    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("SELECT val FROM public.seq_lww")
        rows = cur.fetchall()
    assert len(rows) == 1 and rows[0][0] == 'high', \
        'highest-sequence record did not win within the bump'
    assert not _leaks()


def test_coexisting_json_non_dict_comment_is_tolerated(db_cleanup):
    main(CONFIG, input_stream=CatStream(100, version=1))

    # Three co-existing tables whose comments are valid JSON but NOT objects.
    # setup_table_mapping_cache reads every comment in the schema during the
    # reswap; a JSON array/number/string must be skipped (not treated as a path,
    # not raised on).
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE public.jc_arr (id bigint)")
            cur.execute("COMMENT ON TABLE public.jc_arr IS '[1,2,3]'")
            cur.execute("CREATE TABLE public.jc_num (id bigint)")
            cur.execute("COMMENT ON TABLE public.jc_num IS '42'")
            cur.execute("CREATE TABLE public.jc_str (id bigint)")
            cur.execute("COMMENT ON TABLE public.jc_str IS '\"hello\"'")

    main(CONFIG, input_stream=CatStream(30, version=2))
    assert _count('cats') == 30
    assert _version('cats') == 2
    assert {'jc_arr', 'jc_num', 'jc_str'}.issubset(_tables())
    assert not _leaks()


def test_non_json_comment_on_own_stream_table_raises_cleanly(db_cleanup):
    main(CONFIG, input_stream=CatStream(50, version=1))
    assert _count('cats') == 50

    # Corrupt the live root's OWN metadata comment to non-JSON. The swap path reads
    # this table's own comment via _get_table_metadata, which is (deliberately) not
    # tolerant -- it must raise rather than silently mis-handle its own metadata.
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("COMMENT ON TABLE public.cats IS 'not json at all'")

    raised = False
    try:
        main(CONFIG, input_stream=CatStream(20, version=2))
    except Exception:
        raised = True
    assert raised, 'a non-JSON comment on the stream\'s own table must raise'

    # Clean failure: the run rolled back, live data is untouched and nothing leaked.
    assert _count('cats') == 50
    assert not _leaks()


def test_preexisting_empty_subtable_fills_in_later_version(db_cleanup):
    props = {
        'id': {'type': ['integer']},
        'items': {'type': ['null', 'array'],
                  'items': {'type': ['object'],
                            'properties': {'sku': {'type': ['string']}}}},
    }
    # v1: the array property exists in the SCHEMA but every array is empty. The
    # subtable is still created from the schema, with zero rows.
    v1 = RecordsStream('orders', props,
                       [{'id': i, 'items': []} for i in range(1, 6)], version=1)
    main(CONFIG, input_stream=v1)
    assert 'orders__items' in _tables()
    assert _count('orders__items') == 0
    assert _version('orders__items') == 1

    # v2: the same subtable now carries rows. It is a live-exists swap (3-step),
    # not a fresh promote, and must end holding exactly v2's rows.
    v2 = RecordsStream('orders', props,
                       [{'id': i, 'items': [{'sku': 'a'}, {'sku': 'b'}, {'sku': 'c'}]}
                        for i in range(1, 6)], version=2)
    main(CONFIG, input_stream=v2)
    assert _count('orders') == 5
    assert _count('orders__items') == 15
    assert _version('orders__items') == 2
    assert _version('orders') == 2
    assert not _leaks()


def test_deep_object_to_array_subtable_promoted_in_later_version(db_cleanup):
    v1_props = {
        'id': {'type': ['integer']},
        'profile': {'type': ['object'],
                    'properties': {'name': {'type': ['string']}}},
    }
    # v1: a deep object, NO array underneath -> profile.name denests to a column,
    # no subtable exists.
    v1 = RecordsStream('orders_deep', v1_props,
                       [{'id': i, 'profile': {'name': 'n'}} for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    assert 'orders_deep' in _tables()
    assert 'orders_deep__profile__contacts' not in _tables()

    # v2 adds an array UNDER the object -> a brand-new nested subtable with no live
    # counterpart. activate_version must promote it via a single rename
    # (live_exists=False branch).
    v2_props = {
        'id': {'type': ['integer']},
        'profile': {'type': ['object'],
                    'properties': {
                        'name': {'type': ['string']},
                        'contacts': {'type': ['null', 'array'],
                                     'items': {'type': ['object'],
                                               'properties': {'email': {'type': ['string']}}}}}},
    }
    v2 = RecordsStream('orders_deep', v2_props,
                       [{'id': i, 'profile': {'name': 'n',
                                              'contacts': [{'email': 'a'}, {'email': 'b'}]}}
                        for i in range(1, 6)], version=2)
    main(CONFIG, input_stream=v2)
    assert _count('orders_deep') == 5
    assert 'orders_deep__profile__contacts' in _tables()
    assert _count('orders_deep__profile__contacts') == 10
    assert _version('orders_deep') == 2
    assert not _leaks()


def test_staging_overflow_collision_suffix_while_live_fits(db_cleanup):
    name = 'staging_overflow_' + 'x' * 38
    assert len(name) == 55
    assert len('pqtemp__' + name + '__2') > 63   # 66 -> staging root truncates
    assert len(name + '__q') < 63                # 58 -> live subtable fits cleanly
    props = {
        'id': {'type': ['integer']},
        'q': {'type': ['null', 'array'], 'items': {'type': ['integer']}},
    }
    v1 = RecordsStream(name, props,
                       [{'id': i, 'q': [1, 2]} for i in range(1, 6)], version=1)
    main(CONFIG, input_stream=v1)
    assert name in _tables()
    assert (name + '__q') in _tables()

    # Staging root pqtemp__<name>__2 (66) truncates to 63 and its subtable
    # collides to the same 63 (gaining a __1 suffix the live subtable never had).
    # The path-keyed swap must still land them on the clean live names.
    v2 = RecordsStream(name, props,
                       [{'id': i, 'q': [7, 8, 9]} for i in range(1, 5)], version=2)
    main(CONFIG, input_stream=v2)
    assert _count(name) == 4
    assert _count(name + '__q') == 12
    assert _version(name) == 2
    assert _version(name + '__q') == 2
    assert not _leaks()


def test_two_subtables_canonicalize_to_same_name_suffixed_reswap(db_cleanup):
    # 'a-b' and 'a_b' both canonicalize to subtable base 'w__a_b'; the second
    # collides and gets a '__1' suffix. Both must reswap independently on a bump.
    props = {
        'id': {'type': ['integer']},
        'a-b': {'type': ['null', 'array'], 'items': {'type': ['integer']}},
        'a_b': {'type': ['null', 'array'], 'items': {'type': ['integer']}},
    }
    v1 = RecordsStream('w', props,
                       [{'id': i, 'a-b': [1], 'a_b': [2]} for i in range(1, 6)],
                       version=1)
    main(CONFIG, input_stream=v1)
    subs = {t for t in _tables() if t.startswith('w__')}
    assert subs == {'w__a_b', 'w__a_b__1'}, subs

    v2 = RecordsStream('w', props,
                       [{'id': i, 'a-b': [1], 'a_b': [2]} for i in range(1, 6)],
                       version=2)
    main(CONFIG, input_stream=v2)
    assert {t for t in _tables() if t.startswith('w__')} == {'w__a_b', 'w__a_b__1'}
    assert _count('w') == 5
    assert _count('w__a_b') == 5
    assert _count('w__a_b__1') == 5
    assert _version('w') == 2
    assert _version('w__a_b') == 2
    assert _version('w__a_b__1') == 2
    assert not _leaks()


def test_swap_failure_leaves_live_unswapped_and_orphans_staging(db_cleanup):
    main(CONFIG, input_stream=CatStream(20, version=1, nested_count=0))
    assert _count('cats') == 20

    # Forge the exact intermediate name the 3-step swap renames the live root to at
    # step 1 (`cats__old`). Its presence makes the first ALTER collide and abort.
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE public.cats__old (id bigint)")

    raised = False
    try:
        main(CONFIG, input_stream=CatStream(8, version=2, nested_count=0))
    except Exception:
        raised = True
    assert raised, 'the colliding intermediate name must abort the swap'

    # The root swap never committed: live `cats` still holds the v1 rows, and this
    # run's staging root remains orphaned (cleanup is a separate concern).
    assert _count('cats') == 20, 'live root was swapped despite the failure'
    assert 'pqtemp__cats__2' in _tables(), 'failed run did not leave staging behind'

    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS public.cats__old")
