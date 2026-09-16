"""Exercises Database against a real MariaDB server -- not a mocked cursor.

MariaDB is wire- and SQL-compatible with MySQL for everything this library does
(see MariaDBDialect's docstring), so this mirrors test_integration_mysql.py
almost exactly -- same driver (mysql.connector), same DDL, same assertions --
just pointed at a MariaDB server/port and DatabaseType.MARIADB instead.

Requires a MariaDB server reachable at the settings below (see docker-compose.yml:
`docker compose up -d mariadb`) and mysql-connector-python installed (the
`mariadb` extra). Skipped automatically, with a clear reason, if either isn't
available. Excluded from the default `pytest` run (see pyproject.toml's
addopts) -- run explicitly with `pytest -m integration`.
"""
import uuid

import pytest

pytest.importorskip('mysql.connector', reason='mysql-connector-python is not installed (pip install -e ".[mariadb]")')

from lightweight_etl.memory import DatabaseMemory
from lightweight_etl.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from lightweight_etl.database import Database
from lightweight_etl.memory import FileMemory
from lightweight_etl.runner import runDataJobs

pytestmark = pytest.mark.integration

CONNECTION_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.MARIADB, user='root', password='root', database='lightweight_etl_test', host='127.0.0.1', port=3308,
    )


@pytest.fixture
def liveDatabase():
    try:
        database = Database(connectionSettings=CONNECTION_SETTINGS)
    except Exception as error:
        pytest.skip(f'no live mariadb server reachable at {CONNECTION_SETTINGS.host}:{CONNECTION_SETTINGS.port} ({error})')

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

    liveDatabase.alter('CREATE TABLE {} (job VARCHAR(255) PRIMARY KEY, last_run DOUBLE, watermark_value VARCHAR(255), watermark_type VARCHAR(32))'.format(tableName))

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


def test_truncate_removes_all_rows_but_keeps_the_table(liveDatabase, peopleTable):
    liveDatabase.insert(table=peopleTable, data=[(1, 'alice', 100)])

    liveDatabase.truncate(table=peopleTable)

    assert liveDatabase.query('SELECT * FROM {}'.format(peopleTable)) == []
    assert liveDatabase.getAllColumnNames(table=peopleTable) == ['id', 'name', 'amount']


def test_run_data_jobs_end_to_end_against_a_real_server(liveDatabase, peopleTable, tmp_path):
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

    runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': CONNECTION_SETTINGS}, logFile=tmp_path / 'runner.log',
                memory=FileMemory(memoryFile=memoryPath), runForever=False)

    rows = liveDatabase.query('SELECT id, name, amount FROM {} ORDER BY id'.format(peopleTable))
    assert rows == [(1, 'old', 1), (2, 'new', 2)]
    assert 'job1' in FileMemory(memoryFile=memoryPath).read()


def test_database_memory_records_and_reads_back_a_run(memoryTable):
    memory = DatabaseMemory(connectionSettings=CONNECTION_SETTINGS, table=memoryTable)

    memory.recordRun(job='job1')

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
