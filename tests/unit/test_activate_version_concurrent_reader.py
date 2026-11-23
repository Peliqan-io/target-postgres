"""
Regression test for PQ-4755: activate_version() must never commit a state in
which a live table carries its staging path.

The swap renames pqtemp__<stream>__<version> onto the live name and commits. If
the renamed table's comment still holds the staging path at that commit, then a
second target-postgres process that rebuilds its mapping cache before the path
is repaired finds no table for (<stream>,), sees the natural name already taken,
and allocates <stream>__1 (NAME COLLISION). Once the first process repairs the
original, two tables claim the same path and loads silently diverge.

The test plays that second process. After every commit the loading target makes,
an independent PostgresTarget rebuilds its mapping cache from committed state
and resolves each live path the way write_batch() does. Every path must resolve
to its real live table, never a fresh collision-suffixed name.
"""
import json

import psycopg2
import pytest

from target_postgres import main, postgres
from utils.fixtures import CatStream, CONFIG, TEST_DB, db_cleanup

LIVE_PATHS = {
    ('cats',): 'cats',
    ('cats', 'adoption', 'immunizations'): 'cats__adoption__immunizations',
}


@pytest.fixture
def concurrent_reader(monkeypatch):
    # Built before commit is patched, so its own setup does not trigger observing.
    reader = postgres.PostgresTarget(CONFIG)
    observations = []
    in_observer = []
    real_commit = postgres.MillisLoggingConnection.commit

    def observe():
        with reader.conn.cursor() as cur:
            reader.setup_table_mapping_cache(cur)
        reader.conn.rollback()
        observations.append({
            path: reader.add_table_mapping_helper(path, reader.table_mapping_cache)['to']
            for path in LIVE_PATHS
        })

    def commit_then_observe(conn):
        real_commit(conn)
        if conn is not reader.conn and not in_observer:
            in_observer.append(True)
            try:
                observe()
            finally:
                in_observer.pop()

    def start():
        monkeypatch.setattr(postgres.MillisLoggingConnection, 'commit', commit_then_observe)

    yield start, observations
    reader.conn.close()


def test_version_bump_never_exposes_staging_path_to_concurrent_reader(db_cleanup, concurrent_reader):
    start_observing, observations = concurrent_reader

    main(CONFIG, input_stream=CatStream(50, version=1, nested_count=2))

    start_observing()
    main(CONFIG, input_stream=CatStream(30, version=2, nested_count=2))

    assert observations, 'reader never observed a commit'
    for i, resolved in enumerate(observations):
        assert resolved == LIVE_PATHS, (
            'after commit #{}, a concurrent loader would resolve {}'.format(i, resolved))


def test_failed_swap_leaves_no_second_claim_on_live_path(db_cleanup):
    # The clean path is written onto the staging table before the swap. If that
    # write committed separately, an aborted swap would leave the orphaned staging
    # table claiming the live path next to the live table: the same duplicate.
    main(CONFIG, input_stream=CatStream(20, version=1, nested_count=0))

    # `cats__old` already existing makes the swap's first ALTER fail.
    with psycopg2.connect(**TEST_DB) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE public.cats__old (id bigint)")

    with pytest.raises(Exception):
        main(CONFIG, input_stream=CatStream(8, version=2, nested_count=0))

    with psycopg2.connect(**TEST_DB) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT c.relname, obj_description(c.oid, 'pg_class')
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relkind = 'r'
        """)
        claimants = sorted(name for name, comment in cur.fetchall()
                           if comment and json.loads(comment).get('path') == ['cats'])
    assert claimants == ['cats']
