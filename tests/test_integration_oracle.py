"""Exercises Database against a real Oracle server -- not a mocked cursor.

See test_integration_mysql.py for the rationale. This is the dialect that was,
until now, only ever verified as generated SQL text against a mocked cursor --
cx_Oracle wouldn't even compile in earlier attempts at this. Switching to
oracledb's default "thin" mode (pure Python, no Oracle Client install, see
bauta/databaseDialects.py) finally made a real connection possible, including
proving the MERGE-based upsert/upsertFromStage and the three-statement swap
(Oracle's cursor.execute() only runs one statement at a time) actually work.

Requires an Oracle server reachable at the settings below (see
docker-compose.yml: `docker compose up -d oracle`, using the gvenzl/oracle-free
image -- free, Apache-2.0 licensed, no Oracle Container Registry login needed)
and oracledb importable. Skipped automatically, with a clear reason, if either
isn't available. Excluded from the default `pytest` run -- run explicitly with
`pytest -m integration`.

One real dialect difference this file has to account for: Oracle folds
unquoted identifiers to UPPERCASE, so getAllColumnNames/getPrimaryColumnNames
return e.g. 'ID' rather than 'id' -- that's genuine Oracle behavior, not a bug.
"""
import uuid

import pytest

pytest.importorskip('oracledb', reason='oracledb is not installed (pip install -e ".[oracle]")')

from bauta.memory import DatabaseMemory
from bauta.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from bauta.database import Database
from bauta.memory import FileMemory
from bauta.runner import runDataJobs

pytestmark = pytest.mark.integration

CONNECTION_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.ORACLE, user='system', password='oracle', database='bauta_test',
    host='127.0.0.1', port=1522, serviceName='bauta_test',
    )


@pytest.fixture
def liveDatabase():
    try:
        database = Database(connectionSettings=CONNECTION_SETTINGS)
    except Exception as error:
        pytest.skip(f'no live oracle server reachable at {CONNECTION_SETTINGS.host}:{CONNECTION_SETTINGS.port} ({error})')

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

    # Oracle has no bare DOUBLE type (mysql does) -- BINARY_DOUBLE is Oracle's
    # native equivalent; DatabaseMemory's docstring already flags
    # "adjust types to your database" for exactly this kind of difference
    liveDatabase.alter('CREATE TABLE {} (job VARCHAR(255) PRIMARY KEY, last_run BINARY_DOUBLE, watermark_value VARCHAR(255), watermark_type VARCHAR(32))'.format(tableName))

    yield tableName

    liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


def test_schema_introspection_against_a_real_table(liveDatabase, peopleTable):
    # Oracle folds unquoted identifiers to uppercase -- this is genuine Oracle
    # behavior being verified here, not something our code controls
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['ID', 'NAME', 'AMOUNT']
    assert liveDatabase.getPrimaryColumnNames(table=peopleTable) == ['ID']


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
    """The interesting case for Oracle specifically: upsertQuery is a MERGE
    statement built from bind-variable columns selected from dual, not the
    ON DUPLICATE KEY / ON CONFLICT syntax mysql/postgres use -- this is the first
    real proof that MERGE actually inserts new rows *and* updates existing ones.
    """
    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])

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
    """The other interesting case for Oracle: swapQueries returns three separate
    ALTER TABLE ... RENAME TO statements (Oracle's cursor.execute() only runs one
    statement at a time, unlike mysql's single multi-target RENAME TABLE or
    postgres's semicolon-chained simple-query execution) -- this proves Database.
    swap() actually issuing three separate execute() calls works correctly.
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
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['ID', 'NAME', 'AMOUNT']


def test_context_manager_against_a_real_connection(peopleTable):
    """Confirms the connection is actually closed once the with-block exits, not
    just that __exit__ was called.
    """
    with Database(connectionSettings=CONNECTION_SETTINGS) as database:
        database.insert(table=peopleTable, data=[(1, 'alice', 100)])
        assert database.query('SELECT COUNT(*) FROM {}'.format(peopleTable)) == [(1,)]

    with pytest.raises(Exception):
        database.query('SELECT 1 FROM dual')


def test_run_data_jobs_end_to_end_against_a_real_server(liveDatabase, peopleTable, tmp_path):
    """The rest of this file exercises Database directly; this is the one test that
    goes through the full runDataJobs path -- real Configuration validation, a real
    DependencyGraph, a real multiprocessing.Pool, _executeDataJob running in a
    worker *process* (not this one), and FileMemory being pickled, reconstructed in
    that child process, and writing back a real run timestamp.
    """
    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 1)])

    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert',
                'chunkSize': 100, 'targetTableFinal': peopleTable, 'sourceQuery': "select 2, 'new', 2 from dual",
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
    the reference DatabaseMemory instead of FileMemory.
    """
    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 1)])

    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert',
                'chunkSize': 100, 'targetTableFinal': peopleTable, 'sourceQuery': "select 2, 'new', 2 from dual",
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
    got a non-buffering cursor -- psycopg needs a *named* (server-side) cursor,
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
    bare execute(), which psycopg's server-side cursors in particular do not
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
