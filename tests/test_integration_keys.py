"""Primary keys, upserts and swaps against real servers, where the catalogs differ.

Regression tests for three ways the key lookup used to go wrong, each silently:

- a same-named table in another schema added its key columns, so an upsert
  matched on the wrong columns and left some unwritten;
- UNIQUE columns counted as key columns, so an upsert that changed one failed
  (Oracle) or was refused outright (PostgreSQL);
- a key-only table on MySQL used INSERT IGNORE, which also swallows real errors.

And for swap with schema-qualified names, which renames used to reject.

Parametrized over the five servers in docker-compose.yml; any that isn't
reachable, or whose driver isn't installed, is skipped with a reason. Run with
`pytest -m integration`.
"""
import datetime
import decimal
import importlib
import uuid

import pytest

from bauta.configuration import ConfigurationError
from bauta.database import Database
from servers import SERVERS

pytestmark = pytest.mark.integration

# How to create and drop a second schema on each server, from the connection's
# own. Oracle's schemas are users.
SCHEMAS = {
    'mysql': ('CREATE DATABASE {0}', 'DROP DATABASE IF EXISTS {0}'),
    'mariadb': ('CREATE DATABASE {0}', 'DROP DATABASE IF EXISTS {0}'),
    'postgresql': ('CREATE SCHEMA {0}', 'DROP SCHEMA IF EXISTS {0} CASCADE'),
    'mssql': ('CREATE SCHEMA {0}', 'DROP SCHEMA IF EXISTS {0}'),
    'oracle': ('CREATE USER {0} IDENTIFIED BY "Pw{0}" QUOTA UNLIMITED ON USERS', 'DROP USER {0} CASCADE'),
    }


@pytest.fixture(params=sorted(SERVERS))
def server(request):
    driver, settings = SERVERS[request.param]

    try:
        importlib.import_module(driver)
        database = Database(connectionSettings=settings)
    except Exception as error:
        pytest.skip('{} is not available ({})'.format(request.param, error))

    created = []

    def table(definition: str, schema: str = '') -> str:
        name = '{}t_{}'.format(schema + '.' if schema else '', uuid.uuid4().hex[:8])
        database.alter('CREATE TABLE {} {}'.format(name, definition))
        created.append(name)
        return name

    table.created = created  # type: ignore[attr-defined]

    yield request.param, database, table

    _dropTables(database, created)
    database.close()


def _dropTables(database: Database, tables):
    while tables:
        name = tables.pop()
        try:
            database.alter('DROP TABLE {}'.format(name))
        except Exception:
            database.connection.rollback()


@pytest.fixture
def otherSchema(server):
    serverName, database, _ = server
    name = 'other_{}'.format(uuid.uuid4().hex[:6])
    create, drop = SCHEMAS[serverName]
    database.alter(create.format(name))

    yield name

    # Its tables go first: SQL Server won't drop a schema that still has any.
    created = server[2].created
    inSchema = [table for table in created if table.startswith(name + '.')]
    created[:] = [table for table in created if table not in inSchema]
    _dropTables(database, inSchema)
    database.alter(drop.format(name))


def _rows(database: Database, table: str):
    return sorted(tuple(row) for row in database.query('SELECT * FROM {}'.format(table)))


def test_a_same_named_table_in_another_schema_does_not_lend_its_key(server, otherSchema):
    serverName, database, table = server
    local = table('(a INT PRIMARY KEY, b INT, c INT)')
    unqualified = local.rpartition('.')[2]
    elsewhere = '{}.{}'.format(otherSchema, unqualified)
    database.alter('CREATE TABLE {} (a INT, b INT, c INT, PRIMARY KEY (a, b))'.format(elsewhere))

    try:
        assert [column.lower() for column in database.getPrimaryColumnNames(local)] == ['a']
        assert [column.lower() for column in database.getPrimaryColumnNames(elsewhere)] == ['a', 'b']

        database.upsert(table=local, data=[(1, 2, 3)])
        database.upsert(table=local, data=[(1, 9, 9)])

        assert _rows(database, local) == [(1, 9, 9)]
    finally:
        database.alter('DROP TABLE {}'.format(elsewhere))


def test_a_unique_column_is_not_part_of_the_upsert_key(server):
    _, database, table = server
    people = table('(id INT PRIMARY KEY, email VARCHAR(50) UNIQUE, name VARCHAR(50))')

    assert [column.lower() for column in database.getPrimaryColumnNames(people)] == ['id']

    database.upsert(table=people, data=[(1, 'a@example.test', 'Ann')])
    database.upsert(table=people, data=[(1, 'b@example.test', 'Ann')])

    assert _rows(database, people) == [(1, 'b@example.test', 'Ann')]


def test_a_key_only_table_upserts_idempotently(server):
    _, database, table = server
    links = table('(a INT, b INT, PRIMARY KEY (a, b))')
    stage = table('(a INT, b INT)')
    database.insert(table=stage, data=[(1, 2), (3, 4)])

    for _ in range(2):
        database.upsert(table=links, data=[(1, 2)])
        database.upsertFromStage(targetTable=links, stageTable=stage)

    assert _rows(database, links) == [(1, 2), (3, 4)]


def test_a_key_only_upsert_still_reports_real_errors(server):
    """INSERT IGNORE turned a NULL key into a warning and a silently altered row."""
    _, database, table = server
    links = table('(a INT, b INT, PRIMARY KEY (a, b))')

    with pytest.raises(Exception):
        database.upsert(table=links, data=[(1, None)])


def test_an_upsert_into_a_table_without_a_primary_key_is_refused(server):
    _, database, table = server
    keyless = table('(id INT, name VARCHAR(50))')

    with pytest.raises(ConfigurationError, match='no primary key'):
        database.upsert(table=keyless, data=[(1, 'Ann')])


def test_swap_works_with_schema_qualified_names(server, otherSchema):
    _, database, table = server
    final = table('(id INT PRIMARY KEY)', schema=otherSchema)
    stage = table('(id INT PRIMARY KEY)', schema=otherSchema)
    database.insert(table=final, data=[(1,)])
    database.insert(table=stage, data=[(2,), (3,)])

    database.swap(targetTable=final, stageTable=stage)

    assert _rows(database, final) == [(2,), (3,)]
    assert _rows(database, stage) == [(1,)]
    assert database.tableExists(final)
    assert not database.tableExists('{}.{}_tmp'.format(otherSchema, final.rpartition('.')[2]))


def test_database_history_and_key_fingerprints_work_on_every_server(server):
    """The history table's types, and fingerprint rows in the memory table,
    have to be accepted -- and read back -- by every dialect.
    """
    from bauta.dependencyGraph import JobOutcome, JobStatus
    from bauta.memory import DATABASE_MEMORY_SCHEMA, DatabaseMemory
    from bauta.reporting import DATABASE_HISTORY_SCHEMA, DatabaseHistory
    from bauta.runner import RunResult

    serverName, database, table = server
    historyTable = table(DATABASE_HISTORY_SCHEMA.split('bauta_history', 1)[1])
    memoryTable = table(DATABASE_MEMORY_SCHEMA.split('bauta_memory', 1)[1])
    settings = database.connectionSettings

    history = DatabaseHistory(settings, table=historyTable)
    history.append(RunResult(outcomes=[JobOutcome(job='a', status=JobStatus.FAILED, error='x' * 3000, startedAt=1.0e9, finishedAt=1.0e9 + 2.5),
                                       JobOutcome(job='b', status=JobStatus.SKIPPED)]), 'run-1')
    records = history.read(limit=5)

    assert {record['job'] for record in records} == {'a', 'b'}
    failed = next(record for record in records if record['job'] == 'a')
    assert failed['durationSeconds'] == 2.5 and len(failed['error']) == 2000
    assert type(failed['rowCount']) is int and type(failed['attempts']) is int
    assert [record['job'] for record in history.read(job='b')] == ['b']

    memory = DatabaseMemory(settings, table=memoryTable)
    memory.recordRun('a')
    memory.recordWatermark('a', 5)
    memory.recordKeyFingerprint('a', 'abc123')

    assert memory.readKeyFingerprints() == {'a': 'abc123'}
    assert memory.readWatermarks() == {'a': 5}

    watermarks = {'bytes': b'\x00\x00\x07\xd1', 'decimal': decimal.Decimal('12.50'), 'time': datetime.time(10, 30, 5)}
    for job, value in watermarks.items():
        memory.recordWatermark(job, value)
    read = memory.readWatermarks()
    assert {job: (type(read[job]), read[job]) for job in watermarks} == {job: (type(value), value) for job, value in watermarks.items()}
    assert set(memory.read()) == {'a'}


def test_a_reserved_word_column_loads_and_upserts(server):
    """`rank` and `order` are reserved on at least one server each; the column
    is created as `bauta schema` would, and loaded through both paths.
    """
    from bauta.databaseDialects import quoteFolded

    name, database, table = server
    quoted = {column: quoteFolded(database.type, column) for column in ('id', 'rank', 'order')}
    target = table('({id} INT PRIMARY KEY, {rank} INT, {order} VARCHAR(20))'.format(**quoted))

    database.insert(table=target, data=[(1, 10, 'a'), (2, 20, 'b')], chunkSize=10, columns=['id', 'rank', 'order'])
    database.upsert(table=target, data=[(2, 21, 'B'), (3, 30, 'c')], chunkSize=10, columns=['ID', 'Rank', 'order'])

    rows = database.query('SELECT {id}, {rank}, {order} FROM {table} ORDER BY {id}'.format(table=target, **quoted))
    assert [tuple(row) for row in rows] == [(1, 10, 'a'), (2, 21, 'B'), (3, 30, 'c')]


def test_a_mixed_case_column_created_quoted_loads(server):
    """On Oracle and PostgreSQL, a column created as "CustomerId" only answers
    to that exact spelling; an unquoted load used to miss it.
    """
    from bauta.databaseDialects import quoteIdentifier

    name, database, table = server
    column = quoteIdentifier(database.type, 'CustomerId')
    target = table('(id INT PRIMARY KEY, {} INT)'.format(column))

    database.upsert(table=target, data=[(1, 5)], chunkSize=10, columns=['id', 'customerid'])

    assert [tuple(row) for row in database.query('SELECT id, {} FROM {}'.format(column, target))] == [(1, 5)]
