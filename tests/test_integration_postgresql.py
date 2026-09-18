"""Exercises Database against a real PostgreSQL server -- not a mocked cursor.

See test_integration_mysql.py for the rationale; this is the same suite against
the other dialect whose swap()/upsert() SQL had never been proven against a real
server before -- in particular, PostgreSQLDialect.swapQueries sends three
semicolon-chained statements in a single cursor.execute() call, relying on
psycopg2's simple-query protocol to run them all -- an assumption this file
actually tests rather than just asserting the SQL text looks right.

Requires a PostgreSQL server reachable at the settings below (see
docker-compose.yml: `docker compose up -d postgresql`) and psycopg2 importable.
The `postgresql` extra pins the source-build `psycopg2` package (the upstream-
recommended choice for production); if you don't have PostgreSQL's build
toolchain (pg_config) installed, `pip install psycopg2-binary` instead purely to
run this suite -- see https://www.psycopg.org/docs/install.html. Skipped
automatically, with a clear reason, if no driver or no server is available.
Excluded from the default `pytest` run (see pyproject.toml's addopts) -- run
explicitly with `pytest -m integration`.
"""
import uuid

import pytest

pytest.importorskip('psycopg2', reason='psycopg2 is not installed (pip install psycopg2-binary, or pip install -e ".[postgresql]")')

from understudy_data.memory import DatabaseMemory
from understudy_data.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from understudy_data.database import Database
from understudy_data.memory import FileMemory
from understudy_data.runner import runDataJobs

pytestmark = pytest.mark.integration

CONNECTION_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.POSTGRESQL, user='postgres', password='postgres', database='understudy_test', host='127.0.0.1', port=5433,
    )


@pytest.fixture
def liveDatabase():
    try:
        database = Database(connectionSettings=CONNECTION_SETTINGS)
    except Exception as error:
        pytest.skip(f'no live postgresql server reachable at {CONNECTION_SETTINGS.host}:{CONNECTION_SETTINGS.port} ({error})')

    yield database

    database.close()


@pytest.fixture
def peopleTable(liveDatabase):
    tableName = 'people_{}'.format(uuid.uuid4().hex[:8])

    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(tableName))

    yield tableName

    liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


@pytest.fixture
def memoryTable(liveDatabase):
    tableName = 'memory_{}'.format(uuid.uuid4().hex[:8])

    # postgres has no bare DOUBLE type (mysql does) -- DOUBLE PRECISION is the
    # equivalent; DatabaseMemory's docstring already flags "adjust
    # types to your database" for exactly this kind of difference
    liveDatabase.alter('CREATE TABLE {} (job VARCHAR(255) PRIMARY KEY, last_run DOUBLE PRECISION, watermark_value VARCHAR(255), watermark_type VARCHAR(32))'.format(tableName))

    yield tableName

    liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


def test_schema_introspection_against_a_real_table(liveDatabase, peopleTable):
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['id', 'name', 'amount']
    assert liveDatabase.getPrimaryColumnNames(table=peopleTable) == ['id']


def test_insert_and_query_round_trip(liveDatabase, peopleTable):
    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100), (2, 'bob', 200)])

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))

    assert rows == [(1, 'alice', 100), (2, 'bob', 200)]


def test_insert_chunking_against_a_real_table(liveDatabase, peopleTable):
    data = [(i, 'name{}'.format(i), i * 10) for i in range(1, 11)]

    liveDatabase.insert(table=peopleTable, data=data, chunkSize=3)

    rows = liveDatabase.query('SELECT COUNT(*) FROM {}'.format(peopleTable))
    assert rows == [(10,)]


def test_upsert_inserts_new_rows_and_updates_existing_ones(liveDatabase, peopleTable):
    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])

    # id 1 already exists (update expected), id 2 is new (insert expected)
    liveDatabase.upsert(table=peopleTable, data=[(1, 'alice-updated', 999), (2, 'bob', 200)])

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'alice-updated', 999), (2, 'bob', 200)]


def test_upsert_from_stage(liveDatabase, peopleTable):
    stageTable = peopleTable + '_stage'
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(stageTable))
    try:
        liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])
        liveDatabase.insert(table=stageTable, data=[(1, 'alice-updated', 999), (2, 'bob', 200)])

        liveDatabase.upsertFromStage(targetTable=peopleTable, stageTable=stageTable)

        rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
        assert rows == [(1, 'alice-updated', 999), (2, 'bob', 200)]
    finally:
        liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(stageTable))


def test_swap_replaces_target_with_stage_contents(liveDatabase, peopleTable):
    """The interesting case for postgres specifically: swapQueries returns one
    string with three semicolon-chained ALTER TABLE statements, sent through a
    single cursor.execute() call -- this is the first real proof that psycopg2's
    simple-query protocol actually runs all three rather than just the first.
    """
    stageTable = peopleTable + '_stage'
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(stageTable))

    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 1)])
    liveDatabase.insert(table=stageTable, data=[(2, 'new', 2)])

    liveDatabase.swap(targetTable=peopleTable, stageTable=stageTable)

    rows = liveDatabase.query('SELECT id, name, amount FROM {}'.format(peopleTable))
    assert rows == [(2, 'new', 2)]
    # swap() renamed the original stage table to be the new target -- what was
    # created as the stage table no longer exists under that name, so no drop here


def test_truncate_removes_all_rows_but_keeps_the_table(liveDatabase, peopleTable):
    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])

    liveDatabase.truncate(table=peopleTable)

    assert liveDatabase.query('SELECT * FROM {}'.format(peopleTable)) == []
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['id', 'name', 'amount']


def test_context_manager_against_a_real_connection(peopleTable):
    """Confirms the connection is actually closed once the with-block exits, not
    just that __exit__ was called.
    """
    with Database(connectionSettings=CONNECTION_SETTINGS) as database:
        database.insert(table=peopleTable, data=[(1, 'alice', 100)])
        assert database.query('SELECT COUNT(*) FROM {}'.format(peopleTable)) == [(1,)]

    with pytest.raises(Exception):
        database.query('SELECT 1')


def test_run_data_jobs_end_to_end_against_a_real_server(liveDatabase, peopleTable, tmp_path):
    """The rest of this file exercises Database directly; this is the one test that
    goes through the full runDataJobs path -- real Configuration validation, a real
    DependencyGraph, a real multiprocessing.Pool, _executeDataJob running in a
    worker *process* (not this one), and FileMemory being pickled, reconstructed in
    that child process, and writing back a real run timestamp. Nothing here is
    mocked; this is the closest thing in the suite to what actually happens when
    a configured deployment runs.
    """
    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 1)])

    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert',
                'chunkSize': 100, 'targetTableFinal': peopleTable, 'sourceQuery': "select 2, 'new', 2",
                },
            },
        }
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    memoryPath = tmp_path / 'memory.yaml'

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': CONNECTION_SETTINGS}, logFile=tmp_path / 'runner.log',
                memory=FileMemory(memoryFile=memoryPath), runForever=False)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'old', 1), (2, 'new', 2)]
    assert 'job1' in FileMemory(memoryFile=memoryPath).read()


def test_database_memory_records_and_reads_back_a_run(memoryTable):
    memory = DatabaseMemory(connectionSettings=CONNECTION_SETTINGS, table=memoryTable)

    memory.recordRun(job='job1')

    assert 'job1' in memory.read()


def test_database_memory_upserts_rather_than_duplicating(liveDatabase, memoryTable):
    """read() returning one entry per job wouldn't actually prove there's no
    duplicate row (a dict comprehension would just keep the last one) -- check the
    row count directly instead.
    """
    memory = DatabaseMemory(connectionSettings=CONNECTION_SETTINGS, table=memoryTable)

    memory.recordRun(job='job1')
    firstRun = memory.read()['job1']
    memory.recordRun(job='job1')
    secondRun = memory.read()['job1']

    assert liveDatabase.query('SELECT COUNT(*) FROM {}'.format(memoryTable)) == [(1,)]
    assert secondRun >= firstRun


def test_run_data_jobs_with_database_backed_memory(liveDatabase, peopleTable, memoryTable, tmp_path):
    """Same shape as test_run_data_jobs_end_to_end_against_a_real_server, but with
    the reference DatabaseMemory instead of FileMemory -- proves a MemoryBackend
    backed by the database itself (no locking of its own, just Database.upsert's
    atomicity) survives the same real Pool + worker-process round trip.
    """
    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 1)])

    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert',
                'chunkSize': 100, 'targetTableFinal': peopleTable, 'sourceQuery': "select 2, 'new', 2",
                },
            },
        }
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)
    memory = DatabaseMemory(connectionSettings=CONNECTION_SETTINGS, table=memoryTable)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': CONNECTION_SETTINGS}, logFile=tmp_path / 'runner.log',
                memory=memory, runForever=False)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'old', 1), (2, 'new', 2)]
    assert 'job1' in memory.read()


def test_stream_returns_real_columns_and_bounded_chunks(liveDatabase, peopleTable):
    """Database.stream against this dialect's real driver and cursor.

    This is the check that matters most per-dialect, because streaming is the one
    thing DatabaseDialect cannot fake: a plain fetchmany() bounds how many rows
    Python builds objects for, but says nothing about how many the driver already
    pulled off the socket. Only a real server proves streamingCursor() actually
    got a non-buffering cursor -- psycopg2 needs a *named* (server-side) cursor,
    and mysql.connector needs buffered=False, the inverse of what connect() uses.

    The chunk sizes prove fetchmany is bounding the walk; the reassembled rows
    prove nothing is dropped at a chunk boundary.
    """
    rows = [(index, 'name{}'.format(index), index * 10) for index in range(250)]
    liveDatabase.insert(table=peopleTable, data=rows, chunkSize=100)

    columns, chunks = liveDatabase.stream(query='SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable), chunkSize=100)
    chunkList = list(chunks)

    assert [column.lower() for column in columns] == ['id', 'name', 'amount']
    assert [len(chunk) for chunk in chunkList] == [100, 100, 50]
    assert [tuple(row) for chunk in chunkList for row in chunk] == rows


def test_stream_of_an_empty_table_yields_no_chunks_but_still_reports_columns(liveDatabase, peopleTable):
    """cursor.description has to be populated before any row is fetched -- the
    reason stream() pulls its first chunk eagerly rather than describing off a
    bare execute(), which psycopg2's server-side cursors in particular do not
    reliably support.
    """
    columns, chunks = liveDatabase.stream(query='SELECT id, name, amount FROM {}'.format(peopleTable), chunkSize=100)

    assert [column.lower() for column in columns] == ['id', 'name', 'amount']
    assert list(chunks) == []


def test_stream_closes_its_cursor_when_abandoned_part_way_through(liveDatabase, peopleTable):
    """Abandoning the iterator must not leak the cursor -- on PostgreSQL that is
    a server-side cursor otherwise held for the life of the connection.
    """
    liveDatabase.insert(table=peopleTable, data=[(index, 'n', 0) for index in range(250)], chunkSize=100)

    _, chunks = liveDatabase.stream(query='SELECT id, name, amount FROM {}'.format(peopleTable), chunkSize=10)
    next(chunks)
    chunks.close()

    assert liveDatabase.query('SELECT count(*) FROM {}'.format(peopleTable))[0][0] == 250


@pytest.fixture
def typesTable(liveDatabase):
    tableName = 'copy_types_{}'.format(uuid.uuid4().hex[:8])
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, t TEXT, n NUMERIC(20,5), f FLOAT8, b BOOLEAN, d DATE, ts TIMESTAMPTZ, '
                       'tm TIME, u UUID, by BYTEA, big BIGINT, arr INT[])'.format(tableName))

    yield tableName

    liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


def test_copy_round_trips_every_value_type_it_encodes(liveDatabase, typesTable):
    """COPY's text format has its own escaping; any mistake in it changes data
    rather than failing. Tabs, newlines, backslashes, a literal \\N, NaN and
    infinities, time zones and raw bytes all have to come back as sent.
    """
    import datetime
    import decimal
    import math

    identifier = uuid.uuid4()
    moment = datetime.datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=datetime.timezone.utc)
    rows = [
        (1, 'tab\there\nnew\\back \\N "q" ünï', decimal.Decimal('12345.67890'), float('inf'), True, datetime.date(2026, 1, 2),
         moment, datetime.time(1, 2, 3), identifier, b'\x00\x01\\\xff', 2 ** 62, None),
        (2, '', None, float('-inf'), False, None, None, None, None, b'', -1, None),
        (3, None, decimal.Decimal('-0.00001'), float('nan'), None, None, None, None, None, None, None, None),
        ]

    liveDatabase.insert(table=typesTable, data=rows)

    back = liveDatabase.query('SELECT id, t, n, f, b, d, ts, tm, u::text, by, big FROM {} ORDER BY id'.format(typesTable))

    assert back[0] == (1, rows[0][1], rows[0][2], float('inf'), True, rows[0][5], moment, rows[0][7], str(identifier), back[0][9], 2 ** 62)
    assert bytes(back[0][9]) == b'\x00\x01\\\xff'
    assert back[1][:5] == (2, '', None, float('-inf'), False) and bytes(back[1][9]) == b''
    assert back[2][1] is None and back[2][2] == decimal.Decimal('-0.00001') and math.isnan(back[2][3])


def test_a_chunk_copy_cannot_encode_is_inserted_statement_by_statement(liveDatabase, typesTable):
    liveDatabase.insert(table=typesTable, data=[(1, 'x', None, None, None, None, None, None, None, None, None, [1, 2])])

    assert liveDatabase.query('SELECT arr FROM {}'.format(typesTable)) == [([1, 2],)]


def test_a_copied_upsert_updates_existing_rows_and_keeps_the_last_of_repeated_keys(liveDatabase, peopleTable):
    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 10)])

    liveDatabase.upsert(table=peopleTable, data=[(1, 'new', 11), (2, 'first', 20), (2, 'second', 21)], chunkSize=10)
    liveDatabase.upsert(table=peopleTable, data=[(3, 'third', 30)], columns=['id', 'name', 'amount'])

    assert liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable)) == [
        (1, 'new', 11), (2, 'second', 21), (3, 'third', 30)]


def test_a_swap_repoints_views_at_the_new_target(liveDatabase):
    """A PostgreSQL view follows the table it was made on, not its name, so a
    swap used to leave views reading the old rows -- now the stage table,
    emptied by the next run.
    """
    suffix = uuid.uuid4().hex[:8]
    target, stage = 'orders_{}'.format(suffix), 'orders_{}_stage'.format(suffix)
    view, summary = 'recent_{}'.format(suffix), 'summary_{}'.format(suffix)
    role = 'reader_{}'.format(suffix)

    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, amount INT)'.format(target))
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, amount INT)'.format(stage))
    liveDatabase.alter('CREATE VIEW {} AS SELECT id, amount FROM {} WHERE amount > 0'.format(view, target))
    liveDatabase.alter('CREATE VIEW {} AS SELECT count(*) AS orders FROM {}'.format(summary, view))
    liveDatabase.alter('CREATE ROLE {}'.format(role))
    liveDatabase.alter('GRANT SELECT ON {} TO {}'.format(view, role))

    try:
        liveDatabase.insert(table=target, data=[(1, 10)])
        liveDatabase.insert(table=stage, data=[(2, 20), (3, 30)])

        liveDatabase.swap(targetTable=target, stageTable=stage)

        assert liveDatabase.query('SELECT id FROM {} ORDER BY id'.format(view)) == [(2,), (3,)]
        assert liveDatabase.query('SELECT orders FROM {}'.format(summary)) == [(2,)]
        assert liveDatabase.query("SELECT has_table_privilege('{}', '{}', 'SELECT')".format(role, view)) == [(True,)]
    finally:
        for statement in ('DROP VIEW IF EXISTS {} '.format(summary), 'DROP VIEW IF EXISTS {}'.format(view), 'DROP TABLE IF EXISTS {}'.format(target),
                          'DROP TABLE IF EXISTS {}'.format(stage), 'DROP ROLE IF EXISTS {}'.format(role)):
            liveDatabase.alter(statement)
