"""Exercises Database against a real SQLite file -- not a mocked cursor.

Unlike the other test_integration_*.py files, this needs no docker service and
no optional driver: sqlite3 ships in Python's standard library, and each test
gets its own throwaway file under pytest's tmp_path. That's also why this file
is deliberately *not* marked `integration` (see pyproject.toml's addopts) --
there's no server to be missing, so there's no reason to exclude it from the
default `pytest` run.

Uses a real file path rather than ":memory:" -- runDataJobs opens fresh
Database connections in worker *processes*, and SQLite's ":memory:" database is
private to the connection (and process) that created it, so a worker's own
connection would never see data written through this file's other fixtures.
"""
import uuid

import pytest

from library.configurationInterface import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from library.databaseInterface import Database
from library.memoryInterface import FileMemory
from library.runner import runDataJobs


def _connectionSettings(tmp_path) -> DatabaseConnectionConfig:
    return DatabaseConnectionConfig(type=DatabaseType.SQLITE, database=str(tmp_path / 'test.db'))


@pytest.fixture
def connectionSettings(tmp_path):
    return _connectionSettings(tmp_path)


@pytest.fixture
def liveDatabase(connectionSettings):
    database = Database(connectionSettings=connectionSettings)

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


def test_upsert_inserts_new_rows_and_updates_existing_ones(liveDatabase, peopleTable):
    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])

    liveDatabase.upsert(table=peopleTable, data=[(1, 'alice-updated', 999), (2, 'bob', 200)])

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'alice-updated', 999), (2, 'bob', 200)]


def test_upsert_from_stage(liveDatabase, peopleTable):
    stageTable = peopleTable + '_stage'
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(stageTable))

    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])
    liveDatabase.insert(table=stageTable, data=[(1, 'alice-updated', 999), (2, 'bob', 200)])

    liveDatabase.upsertFromStage(targetTable=peopleTable, stageTable=stageTable)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'alice-updated', 999), (2, 'bob', 200)]


def test_swap_replaces_target_with_stage_contents(liveDatabase, peopleTable):
    stageTable = peopleTable + '_stage'
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(stageTable))

    liveDatabase.insert(table=peopleTable, data=[(1, 'old', 1)])
    liveDatabase.insert(table=stageTable, data=[(2, 'new', 2)])

    liveDatabase.swap(targetTable=peopleTable, stageTable=stageTable)

    rows = liveDatabase.query('SELECT id, name, amount FROM {}'.format(peopleTable))
    assert rows == [(2, 'new', 2)]


def test_truncate_removes_all_rows_but_keeps_the_table(liveDatabase, peopleTable):
    """SQLite has no TRUNCATE statement -- SQLiteDialect.truncateQuery falls back to
    DELETE FROM with no WHERE clause, which this proves actually clears the table.
    """
    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])

    liveDatabase.truncate(table=peopleTable)

    assert liveDatabase.query('SELECT * FROM {}'.format(peopleTable)) == []
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['id', 'name', 'amount']


def test_run_data_jobs_end_to_end_against_a_real_file(liveDatabase, peopleTable, connectionSettings, tmp_path):
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

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': connectionSettings}, logDirectory=tmp_path / 'runner.log',
                memory=FileMemory(memoryDirectory=memoryPath), runForever=False)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'old', 1), (2, 'new', 2)]
    assert 'job1' in FileMemory(memoryDirectory=memoryPath).read()
