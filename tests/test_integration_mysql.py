"""Exercises Database against a real MySQL server -- not a mocked cursor.

Every other test in this suite proves the SQL text and control flow are
correct; this proves that SQL text actually executes against a real server.

Requires a MySQL server reachable at the settings below (see docker-compose.yml:
`docker compose up -d mysql`) and mysql-connector-python installed (the `mysql`
extra). Skipped automatically, with a clear reason, if either isn't available.
Excluded from the default `pytest` run (see pyproject.toml's addopts) -- run
explicitly with `pytest -m integration`.
"""
import uuid

import pytest

pytest.importorskip('mysql.connector', reason='mysql-connector-python is not installed (pip install -e ".[mysql]")')

from library.configurationInterface import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from library.databaseInterface import Database
from library.memoryInterface import FileMemory
from library.runner import runDataJobs

pytestmark = pytest.mark.integration

CONNECTION_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.MYSQL, user='root', password='root', database='lightweight_etl_test', host='127.0.0.1', port=3307,
    )


@pytest.fixture
def liveDatabase():
    try:
        database = Database(connectionSettings=CONNECTION_SETTINGS)
    except Exception as error:
        pytest.skip(f'no live mysql server reachable at {CONNECTION_SETTINGS.host}:{CONNECTION_SETTINGS.port} ({error})')

    yield database

    database.close()


@pytest.fixture
def peopleTable(liveDatabase):
    tableName = 'people_{}'.format(uuid.uuid4().hex[:8])

    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(tableName))

    yield tableName

    liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(tableName))


def test_schema_introspection_against_a_real_table(liveDatabase, peopleTable):
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['id', 'name', 'amount']
    assert liveDatabase.getPrimaryColumnNames(table=peopleTable) == ['id']
    assert liveDatabase.getNonPrimaryColumnNames(table=peopleTable) == ['name', 'amount']


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
    example_jobs.py runs.
    """
    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 1)])

    raw = {
        'workers': 1,
        'jobs': {
            'job1': {
                'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert',
                'chunkSize': 100, 'targetTableFinal': peopleTable, 'sourceQuery': 'select 2, "new", 2',
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
