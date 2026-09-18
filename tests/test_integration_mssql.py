"""Exercises Database against a real SQL Server -- not a mocked cursor.

See test_integration_mysql.py for the rationale. This proves MSSQLDialect's
MERGE-based upsert/upsertFromStage and its chained sp_rename-based swap actually
work against a real server, not just as generated SQL text.

Requires a SQL Server reachable at the settings below (see docker-compose.yml:
`docker compose up -d mssql`, using Microsoft's own official image -- no
Container Registry login needed, just an EULA acceptance env var) and pymssql
importable. Skipped automatically, with a clear reason, if either isn't
available. Excluded from the default `pytest` run -- run explicitly with
`pytest -m integration`.

Uses the always-present `master` database rather than provisioning a dedicated
one (unlike the other three dialects' suites) -- SQL Server's official image has
no equivalent of MYSQL_DATABASE/POSTGRES_DB/ORACLE_DATABASE env vars, and every
test already uses a uniquely-named table, so there's nothing to isolate beyond
that.
"""
import uuid

import pytest

pytest.importorskip('pymssql', reason='pymssql is not installed (pip install -e ".[mssql]")')

from understudy_data.memory import DatabaseMemory
from understudy_data.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from understudy_data.database import Database
from understudy_data.memory import FileMemory
from understudy_data.runner import runDataJobs

pytestmark = pytest.mark.integration

CONNECTION_SETTINGS = DatabaseConnectionConfig(
    type=DatabaseType.MSSQL, user='sa', password='YourStr0ng!Passw0rd', database='master', host='127.0.0.1', port=1434,
    )


@pytest.fixture
def liveDatabase():
    try:
        database = Database(connectionSettings=CONNECTION_SETTINGS)
    except Exception as error:
        pytest.skip(f'no live sql server reachable at {CONNECTION_SETTINGS.host}:{CONNECTION_SETTINGS.port} ({error})')

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

    # SQL Server has no bare DOUBLE type (mysql does) -- FLOAT is its ANSI-ish
    # double-precision equivalent; DatabaseMemory's docstring already
    # flags "adjust types to your database" for exactly this kind of difference
    liveDatabase.alter('CREATE TABLE {} (job VARCHAR(255) PRIMARY KEY, last_run FLOAT, watermark_value VARCHAR(255), watermark_type VARCHAR(32))'.format(tableName))

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
    """The interesting case for SQL Server specifically: upsertQuery is a MERGE
    statement sourced from a VALUES table-value-constructor bound via executemany,
    not the ON DUPLICATE KEY / ON CONFLICT syntax mysql/postgres use -- this is the
    first real proof that MERGE actually inserts new rows *and* updates existing ones.
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
    """The other interesting case for SQL Server: swapQueries chains three EXEC
    sp_rename calls in one execute() (sp_rename is a stored procedure, not DDL, so
    unlike Oracle's three separate ALTER TABLE RENAME statements, these can be
    chained) -- this proves that actually works, not just that it looks right.
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
    that child process, and writing back a real run timestamp.
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
    the reference DatabaseMemory instead of FileMemory.
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


def test_a_rowversion_watermark_survives_database_backed_memory(liveDatabase, memoryTable, tmp_path):
    """rowversion is how SQL Server tracks changes, and arrives as bytes. Stored
    as text, the second run compared binary with a string and failed.
    """
    suffix = uuid.uuid4().hex[:8]
    source, target = 'rv_source_{}'.format(suffix), 'rv_target_{}'.format(suffix)
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name NVARCHAR(20), rv ROWVERSION)'.format(source))
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, name NVARCHAR(20), source_version VARBINARY(8))'.format(target))
    try:
        liveDatabase.alter("INSERT INTO {} (id, name) VALUES (1, 'a'), (2, 'b')".format(source))
        jobsFile = Configuration.validateJobConfiguration({'workers': 1, 'jobs': {'job1': {
            'active': True, 'sourceDatabase': 'db', 'targetDatabase': 'db', 'insertStrategy': 'upsert', 'chunkSize': 10,
            'sourceQuery': 'SELECT id, name, rv FROM {} WHERE rv > {{{{ watermark }}}}'.format(source), 'watermarkColumn': 'rv',
            'watermarkInitial': 0, 'targetTableFinal': target,
            }}}, DataJobsFile)
        memory = DatabaseMemory(connectionSettings=CONNECTION_SETTINGS, table=memoryTable)

        def run():
            result = runDataJobs(jobsFile=jobsFile, databaseConfiguration={'db': CONNECTION_SETTINGS}, logFile=tmp_path / 'runner.log',
                                 memory=memory, runForever=False)
            (outcome,) = result.outcomes
            assert outcome.error is None
            return outcome.rowCount

        assert run() == 2
        assert isinstance(memory.readWatermarks()['job1'], bytes)
        liveDatabase.alter("UPDATE {} SET name = 'B' WHERE id = 2".format(source))
        assert run() == 1
        assert liveDatabase.query('SELECT id, name FROM {} ORDER BY id'.format(target)) == [(1, 'a'), (2, 'B')]
        assert memory.readWatermarks()['job1'] == liveDatabase.query('SELECT MAX(rv) FROM {}'.format(source))[0][0]
    finally:
        for table in (source, target):
            liveDatabase.alter('DROP TABLE {}'.format(table))


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


def test_bulk_loads_round_trip_awkward_values(liveDatabase):
    """Multi-row statements carry values quoted into the SQL by pymssql: quotes,
    percent signs, format markers, Unicode and bytes must all arrive intact.
    """
    import datetime
    import decimal

    table = 'bulk_{}'.format(uuid.uuid4().hex[:8])
    liveDatabase.alter('CREATE TABLE {} (id INT PRIMARY KEY, t NVARCHAR(100), n DECIMAL(12,3), d DATETIME2, b VARBINARY(10))'.format(table))
    try:
        rows = [(1, "it's 100% ünï %s %(x)s", decimal.Decimal('1.250'), datetime.datetime(2026, 1, 2, 3, 4, 5), b'\x00\xff'),
                (2, None, None, None, None)]
        liveDatabase.insert(table=table, data=rows)
        assert liveDatabase.query('SELECT * FROM {} ORDER BY id'.format(table)) == rows

        liveDatabase.upsert(table=table, data=[(1, 'first', None, None, None), (3, 'new', None, None, None), (1, 'last', None, None, None)])
        assert liveDatabase.query('SELECT id, t FROM {} ORDER BY id'.format(table)) == [(1, 'last'), (2, None), (3, 'new')]

        many = [(index, 'n{}'.format(index), None, None, None) for index in range(10, 2510)]
        liveDatabase.insert(table=table, data=many, chunkSize=2500)
        assert liveDatabase.query('SELECT count(*) FROM {}'.format(table)) == [(2503,)]
    finally:
        liveDatabase.alter('DROP TABLE {}'.format(table))
