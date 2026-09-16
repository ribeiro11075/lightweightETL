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

from lightweight_etl.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from lightweight_etl.database import Database
from lightweight_etl.memory import FileMemory
from lightweight_etl.runner import runDataJobs


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

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': connectionSettings}, logFile=tmp_path / 'runner.log',
                memory=FileMemory(memoryFile=memoryPath), runForever=False)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'old', 1), (2, 'new', 2)]
    assert 'job1' in FileMemory(memoryFile=memoryPath).read()


def test_stream_returns_real_columns_and_bounded_chunks(liveDatabase, peopleTable):
    """Database.stream against a real driver/cursor, not a fake: the chunk sizes
    prove fetchmany is actually bounding the walk, and the reassembled rows prove
    nothing is dropped at a chunk boundary.
    """
    rows = [(index, 'name{}'.format(index), index * 10) for index in range(250)]
    liveDatabase.insert(table=peopleTable, data=rows, chunkSize=100)

    columns, chunks = liveDatabase.stream(query='SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable), chunkSize=100)
    chunkList = list(chunks)

    assert columns == ['id', 'name', 'amount']
    assert [len(chunk) for chunk in chunkList] == [100, 100, 50]
    assert [row for chunk in chunkList for row in chunk] == rows


def test_stream_of_an_empty_table_yields_no_chunks_but_still_reports_columns(liveDatabase, peopleTable):
    """Columns have to be trustworthy even with zero rows -- that's what
    Transform.validate() checks against before any load runs.
    """
    columns, chunks = liveDatabase.stream(query='SELECT id, name, amount FROM {}'.format(peopleTable), chunkSize=100)

    assert columns == ['id', 'name', 'amount']
    assert list(chunks) == []


def test_stream_closes_its_cursor_when_abandoned_part_way_through(liveDatabase, peopleTable):
    """Abandoning the iterator (a transform raising mid-stream) must not leak the
    cursor -- on PostgreSQL that would be a server-side cursor held for the life
    of the connection.
    """
    liveDatabase.insert(table=peopleTable, data=[(index, 'n', 0) for index in range(250)], chunkSize=100)

    _, chunks = liveDatabase.stream(query='SELECT id, name, amount FROM {}'.format(peopleTable), chunkSize=10)
    next(chunks)
    chunks.close()

    assert liveDatabase.query('SELECT count(*) FROM {}'.format(peopleTable)) == [(250,)]


def test_run_data_jobs_streams_a_table_larger_than_its_chunk_size(liveDatabase, peopleTable, connectionSettings, tmp_path):
    """End to end through runDataJobs -- a real worker process, a real driver, and
    a chunkSize well below the row count, so the job genuinely streams.
    """
    targetTable = 'people_target_{}'.format(uuid.uuid4().hex[:8])
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), amount INT)'.format(targetTable))
    rows = [(index, 'name{}'.format(index), index) for index in range(500)]
    liveDatabase.insert(table=peopleTable, data=rows, chunkSize=100)

    raw = {'workers': 1, 'jobs': {'copyPeople': {
        'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert',
        'chunkSize': 37, 'targetTableFinal': targetTable,
        'sourceQuery': 'SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable),
        }}}
    jobsFile = Configuration.validateJobConfiguration(raw, DataJobsFile)

    try:
        runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': connectionSettings}, logFile=tmp_path / 'jobs.log',
                    memory=FileMemory(memoryFile=tmp_path / 'jobs.yaml'), runForever=False)

        assert liveDatabase.query('SELECT count(*) FROM {}'.format(targetTable)) == [(500,)]
        assert liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id LIMIT 3'.format(targetTable)) == rows[:3]
    finally:
        liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(targetTable))


@pytest.fixture
def ordersTables(liveDatabase):
    suffix = uuid.uuid4().hex[:8]
    source = 'orders_src_{}'.format(suffix)
    target = 'orders_tgt_{}'.format(suffix)

    for table in (source, target):
        liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR(50), updated_at TEXT)'.format(table))

    yield source, target

    for table in (source, target):
        liveDatabase.alter('DROP TABLE IF EXISTS {}'.format(table))


def _incrementalJobsFile(source, target):
    raw = {'workers': 1, 'jobs': {'loadOrders': {
        'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert', 'chunkSize': 2,
        'targetTableFinal': target, 'watermarkColumn': 'updated_at', 'watermarkInitial': '1970-01-01',
        'sourceQuery': 'SELECT id, name, updated_at FROM {} WHERE updated_at > {{{{ watermark }}}} ORDER BY updated_at'.format(source),
        }}}
    return Configuration.validateJobConfiguration(raw, DataJobsFile)


def test_an_incremental_job_loads_only_rows_past_its_watermark(liveDatabase, ordersTables, connectionSettings, tmp_path):
    """End to end through runDataJobs, twice, against a real database.

    The discriminator is row 1: its name changes in the source between runs but
    its updated_at does not. A full re-extract would pick that change up; a
    genuinely incremental one cannot see the row at all, because the predicate
    now starts past it. Row counts alone would prove nothing here -- upsert is
    idempotent, so re-reading everything would produce an identical target.
    """
    source, target = ordersTables
    memoryPath = tmp_path / 'memory.yaml'
    memory = FileMemory(memoryFile=memoryPath)
    jobsFile = _incrementalJobsFile(source, target)

    liveDatabase.insert(table=source, data=[
        (1, 'first', '2026-01-01T00:00:00'),
        (2, 'second', '2026-01-02T00:00:00'),
        (3, 'third', '2026-01-03T00:00:00'),
        ], chunkSize=10)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': connectionSettings}, logFile=tmp_path / 'jobs.log',
                memory=memory, runForever=False)

    assert liveDatabase.query('SELECT count(*) FROM {}'.format(target)) == [(3,)]
    assert memory.readWatermarks() == {'loadOrders': '2026-01-03T00:00:00'}

    liveDatabase.alter("UPDATE {} SET name = 'CHANGED-BUT-NOT-TOUCHED' WHERE id = 1".format(source))
    liveDatabase.insert(table=source, data=[(4, 'fourth', '2026-01-04T00:00:00')], chunkSize=10)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': connectionSettings}, logFile=tmp_path / 'jobs.log',
                memory=memory, runForever=False)

    assert liveDatabase.query('SELECT count(*) FROM {}'.format(target)) == [(4,)]
    assert liveDatabase.query('SELECT name FROM {} WHERE id = 1'.format(target)) == [('first',)]
    assert liveDatabase.query('SELECT name FROM {} WHERE id = 4'.format(target)) == [('fourth',)]
    assert memory.readWatermarks() == {'loadOrders': '2026-01-04T00:00:00'}


def test_an_incremental_job_with_nothing_new_loads_nothing_and_keeps_its_watermark(liveDatabase, ordersTables, connectionSettings, tmp_path):
    """A run that extracts no rows must leave the stored watermark where it is --
    overwriting it with the null the job reached would re-extract everything.
    """
    source, target = ordersTables
    memory = FileMemory(memoryFile=tmp_path / 'memory.yaml')
    jobsFile = _incrementalJobsFile(source, target)

    liveDatabase.insert(table=source, data=[(1, 'first', '2026-01-01T00:00:00')], chunkSize=10)

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': connectionSettings}, logFile=tmp_path / 'jobs.log',
                memory=memory, runForever=False)
    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': connectionSettings}, logFile=tmp_path / 'jobs.log',
                memory=memory, runForever=False)

    assert liveDatabase.query('SELECT count(*) FROM {}'.format(target)) == [(1,)]
    assert memory.readWatermarks() == {'loadOrders': '2026-01-01T00:00:00'}
