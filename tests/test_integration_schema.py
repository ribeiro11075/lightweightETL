"""`schema` and `clear` across every pair of databases.

For each source and target -- SQLite plus the five servers, 36 pairs -- this
creates a parent and a child table on the source with that dialect's own
types, generates and applies the target's tables with `schema`, copies the rows
with a real data job, and compares the values. It's the only way to find the
problems that live between two drivers: a boolean one returns as an integer
that the other refuses, a date one returns as text that the other can't parse.

Then `clear` empties the target, children first, under a live foreign key.

Servers that aren't reachable are skipped. Run with `pytest -m integration`.
"""
import datetime
import decimal
import importlib
import uuid

import pytest

from lightweight_etl.configuration import Configuration, DatabaseConnectionConfig, DatabaseType, DataJobsFile
from lightweight_etl.database import Database
from lightweight_etl.runner import _executeDataJob
from lightweight_etl.schema import clearTables, createStatements, readTable
from servers import SERVERS

pytestmark = pytest.mark.integration

# Per source dialect: id, big, amount, ratio, name, code, body, born, seen, flag.
SOURCE_TYPES = {
    DatabaseType.SQLITE: ('INTEGER', 'BIGINT', 'DECIMAL(12,2)', 'REAL', 'VARCHAR(40)', 'CHAR(3)', 'TEXT', 'DATE', 'DATETIME', 'BOOLEAN'),
    DatabaseType.MYSQL: ('INT', 'BIGINT', 'DECIMAL(12,2)', 'DOUBLE', 'VARCHAR(40)', 'CHAR(3)', 'TEXT', 'DATE', 'DATETIME', 'BOOLEAN'),
    DatabaseType.MARIADB: ('INT', 'BIGINT', 'DECIMAL(12,2)', 'DOUBLE', 'VARCHAR(40)', 'CHAR(3)', 'TEXT', 'DATE', 'DATETIME', 'BOOLEAN'),
    DatabaseType.POSTGRESQL: ('INTEGER', 'BIGINT', 'NUMERIC(12,2)', 'DOUBLE PRECISION', 'VARCHAR(40)', 'CHAR(3)', 'TEXT', 'DATE', 'TIMESTAMP',
                              'BOOLEAN'),
    DatabaseType.ORACLE: ('NUMBER(10)', 'NUMBER(19)', 'NUMBER(12,2)', 'BINARY_DOUBLE', 'VARCHAR2(40)', 'CHAR(3)', 'CLOB', 'DATE', 'TIMESTAMP',
                          'NUMBER(1)'),
    DatabaseType.MSSQL: ('INT', 'BIGINT', 'DECIMAL(12,2)', 'FLOAT', 'NVARCHAR(40)', 'CHAR(3)', 'NVARCHAR(MAX)', 'DATE', 'DATETIME2', 'BIT'),
    }

COLUMNS = ('id', 'big', 'amount', 'ratio', 'name', 'code', 'body', 'born', 'seen', 'flag')

LONG_TEXT = 'a long body of text, ' * 300


def sourceRows(databaseType):
    """The same logical rows, in the Python types each driver accepts."""
    born, seen = datetime.date(2026, 1, 2), datetime.datetime(2026, 1, 2, 3, 4, 5)
    if databaseType == DatabaseType.SQLITE:
        born, seen = born.isoformat(), seen.isoformat(sep=' ')

    return [
        (1, 9000000000, decimal.Decimal('12.34'), 0.5, 'alpha', 'abc', LONG_TEXT, born, seen, True),
        (2, None, None, None, 'beta', None, None, None, None, False),
        ]


def normalized(row):
    """Values as comparable across drivers: dates as ISO text, flags as ints."""
    identifier, big, amount, ratio, name, code, body, born, seen, flag = row
    return (
        int(identifier),
        None if big is None else int(big),
        None if amount is None else decimal.Decimal(str(amount)).quantize(decimal.Decimal('0.01')),
        None if ratio is None else float(ratio),
        name,
        None if code is None else code.rstrip(),
        body,
        None if born is None else str(born)[:10],
        None if seen is None else str(seen)[:19].replace('T', ' '),
        int(flag),
        )


def connect(name, tmp_path_factory):
    if name == 'sqlite':
        return DatabaseConnectionConfig(type=DatabaseType.SQLITE, database=str(tmp_path_factory.mktemp('sqlite') / 'schema.db'))

    driver, settings = SERVERS[name]
    try:
        importlib.import_module(driver)
        Database(connectionSettings=settings).close()
    except Exception as error:
        pytest.skip('{} is not available ({})'.format(name, error))

    return settings


NAMES = ['sqlite'] + sorted(SERVERS)


@pytest.mark.parametrize('targetName', NAMES)
@pytest.mark.parametrize('sourceName', NAMES)
def test_schema_creates_target_tables_that_a_copy_loads_into(sourceName, targetName, tmp_path_factory):
    source = connect(sourceName, tmp_path_factory)
    target = connect(targetName, tmp_path_factory) if targetName != sourceName or sourceName != 'sqlite' else source
    suffix = uuid.uuid4().hex[:6]
    parent, child = 'sch_parent_{}'.format(suffix), 'sch_child_{}'.format(suffix)
    parentCopy, childCopy = parent + '_c', child + '_c'
    types = SOURCE_TYPES[source.type]

    with Database(connectionSettings=source) as database:
        database.alter('CREATE TABLE {} ({}, PRIMARY KEY (id))'.format(
            parent, ', '.join('{} {}{}'.format(column, columnType, ' NOT NULL' if column == 'name' else '')
                              for column, columnType in zip(COLUMNS, types))))
        database.alter('CREATE TABLE {} (id {} NOT NULL, parent_id {} NOT NULL, PRIMARY KEY (id), '
                       'CONSTRAINT fk_{} FOREIGN KEY (parent_id) REFERENCES {} (id))'.format(child, types[0], types[0], child, parent))
        database.insert(table=parent, data=sourceRows(source.type), chunkSize=10)
        database.insert(table=child, data=[(10, 1), (11, 1), (12, 2)], chunkSize=10)

        foreignKeys = database.getForeignKeys()
        definitions = [readTable(database, table, foreignKeys) for table in (child, parent)]

    # Copy under different names, so a same-server pair doesn't collide with
    # its own source tables.
    renamed = {parent.upper(): parentCopy, child.upper(): childCopy}
    definitions = [
        definition._replace(
            name=renamed[definition.name.upper()],
            foreignKeys=[foreignKey._replace(table=renamed[foreignKey.table.upper()], referencedTable=renamed[foreignKey.referencedTable.upper()],
                                             name=foreignKey.name + '_c')
                         for foreignKey in definition.foreignKeys])
        for definition in definitions
        ]
    statements = createStatements(source.type, target.type, definitions)

    try:
        with Database(connectionSettings=target) as database:
            for statement in statements:
                database.alter(statement.sql)

            assert database.tableExists(parentCopy) and database.tableExists(childCopy)
            assert [column.lower() for column in database.getDefinedPrimaryKey(parentCopy)] == ['id']
            assert {foreignKey.referencedTable.lower() for foreignKey in database.getForeignKeys()
                    if foreignKey.table.lower() == childCopy.lower()} == {parentCopy.lower()}

        databases = {'source': source, 'target': target}
        for table, copy in ((parent, parentCopy), (child, childCopy)):
            job = Configuration.validateJobConfiguration({'workers': 1, 'jobs': {'copy': {
                'active': True, 'sourceDatabase': 'source', 'targetDatabase': 'target', 'sourceQuery': 'SELECT * FROM {}'.format(table),
                'targetTableFinal': copy, 'insertStrategy': 'upsert', 'chunkSize': 10}}}, DataJobsFile).jobs['copy']
            _executeDataJob('copy', job, databases)

        with Database(connectionSettings=target) as database:
            copied = [normalized(row) for row in database.query('SELECT {} FROM {} ORDER BY id'.format(', '.join(COLUMNS), parentCopy))]
            assert copied == [normalized(row) for row in sourceRows(source.type)]
            assert database.query('SELECT count(*) FROM {}'.format(childCopy))[0][0] == 3

            cleared = clearTables(database, [parentCopy, childCopy])
            assert [table.lower() for table, _ in cleared] == [childCopy.lower(), parentCopy.lower()]
            assert database.query('SELECT count(*) FROM {}'.format(parentCopy))[0][0] == 0
    finally:
        for settings, tables in ((target, (childCopy, parentCopy)), (source, (child, parent))):
            with Database(connectionSettings=settings) as database:
                for table in tables:
                    database.alter('DROP TABLE IF EXISTS {}'.format(table))


@pytest.mark.parametrize('name', NAMES)
def test_clear_rolls_back_when_a_table_outside_the_set_still_references_it(name, tmp_path_factory):
    """One transaction: a referencing table that isn't being cleared blocks the
    parent's DELETE, and the child's DELETE before it is undone too.
    """
    settings = connect(name, tmp_path_factory)
    suffix = uuid.uuid4().hex[:6]
    parent, child, other = 'clr_parent_{}'.format(suffix), 'clr_child_{}'.format(suffix), 'clr_other_{}'.format(suffix)

    with Database(connectionSettings=settings) as database:
        if settings.type == DatabaseType.SQLITE:
            database.alter('PRAGMA foreign_keys = ON')
        database.alter('CREATE TABLE {} (id INT PRIMARY KEY)'.format(parent))
        for table in (child, other):
            database.alter('CREATE TABLE {0} (id INT PRIMARY KEY, parent_id INT, CONSTRAINT fk_{0} FOREIGN KEY (parent_id) REFERENCES {1} (id))'.format(
                table, parent))
        database.insert(table=parent, data=[(1,)])
        database.insert(table=child, data=[(1, 1)])
        database.insert(table=other, data=[(1, 1)])

        try:
            with pytest.raises(Exception):
                clearTables(database, [parent, child])

            assert database.query('SELECT count(*) FROM {}'.format(child))[0][0] == 1
            assert database.query('SELECT count(*) FROM {}'.format(parent))[0][0] == 1
        finally:
            for table in (child, other, parent):
                database.alter('DROP TABLE IF EXISTS {}'.format(table))
