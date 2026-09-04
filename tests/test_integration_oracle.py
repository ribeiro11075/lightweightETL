"""Exercises Database against a real Oracle server -- not a mocked cursor.

See test_integration_mysql.py for the rationale. This is the dialect that was,
until now, only ever verified as generated SQL text against a mocked cursor --
cx_Oracle wouldn't even compile in earlier attempts at this. Switching to
oracledb's default "thin" mode (pure Python, no Oracle Client install, see
library/databaseDialects.py) finally made a real connection possible, including
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

from example.example_database_memory import DatabaseMemory
from library.configurationInterface import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from library.databaseInterface import Database
from library.memoryInterface import FileMemory
from library.runner import runDataJobs

pytestmark = pytest.mark.integration

CONNECTION_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.ORACLE, user='system', password='oracle', database='lightweight_etl_test',
    host='127.0.0.1', port=1522, serviceName='lightweight_etl_test',
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
    # native equivalent; example_database_memory.py's docstring already flags
    # "adjust types to your database" for exactly this kind of difference
    liveDatabase.alter('CREATE TABLE {} (job VARCHAR(255) PRIMARY KEY, last_run BINARY_DOUBLE NOT NULL)'.format(tableName))

    yield tableName

    liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


def test_schema_introspection_against_a_real_table(liveDatabase, peopleTable):
    # Oracle folds unquoted identifiers to uppercase -- this is genuine Oracle
    # behavior being verified here, not something our code controls
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['ID', 'NAME', 'AMOUNT']
    assert liveDatabase.getPrimaryColumnNames(table=peopleTable) == ['ID']
    assert liveDatabase.getNonPrimaryColumnNames(table=peopleTable) == ['NAME', 'AMOUNT']


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

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': CONNECTION_SETTINGS}, logDirectory=tmp_path / 'runner.log',
                memory=FileMemory(memoryDirectory=memoryPath), runForever=False)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'old', 1), (2, 'new', 2)]
    assert 'job1' in FileMemory(memoryDirectory=memoryPath).read()


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

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': CONNECTION_SETTINGS}, logDirectory=tmp_path / 'runner.log',
                memory=memory, runForever=False)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'old', 1), (2, 'new', 2)]
    assert 'job1' in memory.read()
