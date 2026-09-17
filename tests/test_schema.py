"""Type mapping and DDL generation, without a server. The cross-database proof
is tests/test_integration_schema.py; these pin the individual decisions.
"""
import sqlite3

import pytest

from understudy_data.configuration import DatabaseConnectionConfig, DatabaseType
from understudy_data.database import Database
from understudy_data.databaseDialects import ColumnDefinition, ForeignKey
from understudy_data.schema import (PortableType, SchemaError, TableDefinition, clearOrder, clearTables, createStatements, orderParentsFirst,
                                    portableType, readTable, renderScript, renderType)
from understudy_data.subset import relatedTables

ORACLE = DatabaseType.ORACLE
POSTGRESQL = DatabaseType.POSTGRESQL
MYSQL = DatabaseType.MYSQL
MSSQL = DatabaseType.MSSQL
SQLITE = DatabaseType.SQLITE


def column(dataType, length=None, precision=None, scale=None, nullable=True, name='c'):
    return ColumnDefinition(name=name, dataType=dataType, length=length, precision=precision, scale=scale, nullable=nullable)


@pytest.mark.parametrize('source,definition,kind', [
    (ORACLE, column('NUMBER', precision=None, scale=0), 'bigint'),
    (ORACLE, column('NUMBER', precision=5, scale=0), 'integer'),
    (ORACLE, column('NUMBER', precision=12, scale=2), 'decimal'),
    (ORACLE, column('NUMBER'), 'decimal'),
    (ORACLE, column('DATE'), 'timestamp'),
    (ORACLE, column('TIMESTAMP(6) WITH TIME ZONE'), 'timestampTz'),
    (ORACLE, column('CLOB'), 'text'),
    (POSTGRESQL, column('character varying', length=40), 'text'),
    (POSTGRESQL, column('timestamp with time zone'), 'timestampTz'),
    (POSTGRESQL, column('boolean'), 'boolean'),
    (POSTGRESQL, column('uuid'), 'uuid'),
    (POSTGRESQL, column('jsonb'), 'json'),
    (MYSQL, column('tinyint'), 'smallint'),
    (MYSQL, column('bit'), 'bigint'),
    (MYSQL, column('longtext', length=4294967295), 'text'),
    (MSSQL, column('bit'), 'boolean'),
    (MSSQL, column('uniqueidentifier'), 'uuid'),
    (MSSQL, column('money'), 'decimal'),
    (SQLITE, column('VARCHAR', length=40), 'text'),
    (SQLITE, column('BOOLEAN'), 'smallint'),
    (SQLITE, column('DATETIME'), 'timestamp'),
    (SQLITE, column('DECIMAL', precision=10, scale=2), 'decimal'),
    (SQLITE, column('BIGINT'), 'bigint'),
    ])
def test_source_types_map_to_portable_ones(source, definition, kind):
    assert portableType(source, definition).kind == kind


def test_unbounded_text_stays_unbounded():
    assert portableType(MYSQL, column('text', length=65535)).length is None
    assert portableType(MSSQL, column('nvarchar', length=-1)).length is None


def test_booleans_stored_as_integers_stay_integers():
    """PostgreSQL refuses an integer in a BOOLEAN column, so only sources whose
    drivers return real booleans produce one.
    """
    for source, name in ((SQLITE, 'BOOLEAN'), (MYSQL, 'boolean'), (MYSQL, 'bit')):
        portable = portableType(source, column(name))
        assert portable.kind != 'boolean'
        assert portable.note


def test_an_unknown_type_becomes_text_with_a_note():
    portable = portableType(POSTGRESQL, column('tsvector'))

    assert portable.kind == 'text'
    assert 'tsvector' in portable.note


@pytest.mark.parametrize('target,portable,expected', [
    (POSTGRESQL, PortableType('decimal', precision=12, scale=2), 'NUMERIC(12,2)'),
    (POSTGRESQL, PortableType('decimal'), 'NUMERIC'),
    (MYSQL, PortableType('decimal', precision=70, scale=40), 'DECIMAL(65,30)'),
    (MYSQL, PortableType('text'), 'LONGTEXT'),
    (MYSQL, PortableType('text', length=40), 'VARCHAR(40)'),
    (MSSQL, PortableType('text', length=5000), 'NVARCHAR(MAX)'),
    (MSSQL, PortableType('boolean'), 'BIT'),
    (ORACLE, PortableType('text', length=40), 'VARCHAR2(40 CHAR)'),
    (ORACLE, PortableType('text'), 'CLOB'),
    (ORACLE, PortableType('bigint'), 'NUMBER(19)'),
    (SQLITE, PortableType('float'), 'REAL'),
    ])
def test_portable_types_render_per_target(target, portable, expected):
    assert renderType(target, portable, isKey=False)[0] == expected


@pytest.mark.parametrize('target,expected', [(MYSQL, 'VARCHAR(255)'), (MSSQL, 'NVARCHAR(255)'), (ORACLE, 'VARCHAR2(255 CHAR)'),
                                             (POSTGRESQL, 'TEXT'), (SQLITE, 'TEXT')])
def test_unbounded_text_in_a_key_is_bounded_where_the_target_requires_it(target, expected):
    rendered, note = renderType(target, PortableType('text'), isKey=True)

    assert rendered == expected
    assert (note is not None) == (expected != 'TEXT')


def test_lossy_renderings_say_so():
    assert renderType(ORACLE, PortableType('time'), isKey=False)[1]
    assert renderType(MYSQL, PortableType('timestampTz'), isKey=False)[1]


def table(name, columns, primaryKey=(), foreignKeys=()):
    return TableDefinition(name=name, columns=[column(dataType, name=columnName, nullable=nullable) for columnName, dataType, nullable in columns],
                           primaryKey=list(primaryKey), foreignKeys=list(foreignKeys))


CUSTOMERS = table('customers', [('id', 'integer', False), ('email', 'text', True)], primaryKey=['id'])
ORDERS = table('orders', [('id', 'integer', False), ('customer_id', 'integer', True)], primaryKey=['id'],
               foreignKeys=[ForeignKey('orders', ('customer_id',), 'customers', ('id',), 'orders_customer_id_fkey')])


def test_statements_create_parents_first_with_keys_and_constraints():
    statements = createStatements(POSTGRESQL, MSSQL, [ORDERS, CUSTOMERS])

    assert [statement.table for statement in statements] == ['customers', 'orders']
    assert '[id] INT NOT NULL' in statements[0].sql
    assert '[email] NVARCHAR(MAX)' in statements[0].sql
    assert 'PRIMARY KEY ([id])' in statements[0].sql
    assert 'CONSTRAINT orders_customer_id_fkey FOREIGN KEY ([customer_id]) REFERENCES customers ([id])' in statements[1].sql


@pytest.mark.parametrize('target,expected', [
    (DatabaseType.ORACLE, '"RANK" NUMBER(10)'), (POSTGRESQL, '"rank" INTEGER'), (DatabaseType.MYSQL, '`Rank` INT'), (MSSQL, '[Rank] INT'),
    ])
def test_column_names_are_quoted_as_the_target_would_store_them_unquoted(target, expected):
    """So a reserved word works, and the column still answers to its unquoted name."""
    definition = table('scores', [('Rank', 'integer', True)], primaryKey=[])

    assert expected in createStatements(POSTGRESQL, target, [definition])[0].sql


def test_a_foreign_key_to_a_table_not_being_created_is_left_out_and_noted():
    [statement] = createStatements(POSTGRESQL, POSTGRESQL, [ORDERS])

    assert 'FOREIGN KEY' not in statement.sql
    assert any('customers is not being created' in note for note in statement.notes)


def test_foreign_keys_can_be_left_out():
    statements = createStatements(POSTGRESQL, POSTGRESQL, [CUSTOMERS, ORDERS], includeForeignKeys=False)

    assert all('FOREIGN KEY' not in statement.sql for statement in statements)


def test_stage_tables_have_the_key_but_no_foreign_keys():
    statements = createStatements(POSTGRESQL, POSTGRESQL, [CUSTOMERS, ORDERS], stageSuffix='_stage')

    assert [statement.table for statement in statements] == ['customers', 'customers_stage', 'orders', 'orders_stage']
    assert 'PRIMARY KEY ("id")' in statements[3].sql
    assert 'FOREIGN KEY' not in statements[3].sql


def test_stages_only_skips_the_tables_themselves():
    statements = createStatements(POSTGRESQL, POSTGRESQL, [CUSTOMERS], stageSuffix='_masked_stage', stagesOnly=True)

    assert [statement.table for statement in statements] == ['customers_masked_stage']


def test_a_primary_key_column_is_never_nullable():
    definition = table('t', [('id', 'integer', True)], primaryKey=['id'])

    assert '"id" INTEGER NOT NULL' in createStatements(POSTGRESQL, POSTGRESQL, [definition])[0].sql


def test_constraint_names_are_made_safe_for_every_dialect():
    foreignKey = ForeignKey('orders', ('customer_id',), 'customers', ('id',), '1 weird-name' + 'x' * 80)
    definition = ORDERS._replace(foreignKeys=[foreignKey])

    sql = createStatements(POSTGRESQL, ORACLE, [CUSTOMERS, definition])[1].sql
    name = sql.split('CONSTRAINT ')[1].split(' ')[0]

    assert name.startswith('fk_1_weird_name')
    assert len(name) == 63


def test_the_script_puts_notes_above_their_table():
    script = renderScript(createStatements(POSTGRESQL, ORACLE, [table('t', [('at', 'time', True)])]), ['heading'])

    assert script.startswith('-- heading\n\n-- at: Oracle has no TIME type')
    assert script.rstrip().endswith(');')


def test_cycles_are_reported_but_self_references_are_not():
    selfReference = [ForeignKey('employees', ('manager_id',), 'employees', ('id',), 'fk')]
    assert orderParentsFirst(['employees'], selfReference) == ['employees']

    cycle = [ForeignKey('a', ('b_id',), 'b', ('id',), 'fk1'), ForeignKey('b', ('a_id',), 'a', ('id',), 'fk2')]
    with pytest.raises(SchemaError, match='cycle among: a, b'):
        orderParentsFirst(['a', 'b'], cycle)


def test_clear_order_is_children_first_and_case_insensitive():
    foreignKeys = [ForeignKey('ORDERS', ('CUSTOMER_ID',), 'CUSTOMERS', ('ID',), 'fk'),
                   ForeignKey('ITEMS', ('ORDER_ID',), 'ORDERS', ('ID',), 'fk2')]

    assert clearOrder(['customers', 'items', 'orders'], foreignKeys) == ['items', 'orders', 'customers']


def test_related_tables_follow_references_both_ways():
    foreignKeys = [ForeignKey('orders', ('customer_id',), 'customers', ('id',), 'fk1'),
                   ForeignKey('orders', ('region_id',), 'regions', ('id',), 'fk2'),
                   ForeignKey('invoices', ('order_id',), 'orders', ('id',), 'fk3')]

    assert relatedTables(foreignKeys, ['customers']) == ['customers', 'invoices', 'orders', 'regions']
    assert relatedTables(foreignKeys, ['ORDERS'], followChildren=False) == ['customers', 'orders', 'regions']


@pytest.fixture
def sqliteDatabase(tmp_path):
    connection = sqlite3.connect(str(tmp_path / 'schema.db'))
    connection.executescript('''
        CREATE TABLE customers (id INTEGER PRIMARY KEY, email VARCHAR(120) NOT NULL, balance DECIMAL(12, 2), active BOOLEAN, note);
        CREATE TABLE orders (id INT, line INT, customer_id INT REFERENCES customers(id), PRIMARY KEY (line, id));
        ''')
    connection.close()

    with Database(connectionSettings=DatabaseConnectionConfig(type=SQLITE, database=str(tmp_path / 'schema.db'))) as database:
        yield database


def test_sqlite_columns_are_read_from_their_declared_types(sqliteDatabase):
    columns = {definition.name: definition for definition in sqliteDatabase.getColumnDefinitions('customers')}

    assert columns['email'] == ColumnDefinition('email', 'VARCHAR', 120, None, None, False)
    assert columns['balance'] == ColumnDefinition('balance', 'DECIMAL', None, 12, 2, True)
    assert columns['id'].nullable is False
    assert columns['note'].dataType == ''


def test_the_defined_primary_key_keeps_its_declared_order(sqliteDatabase):
    assert sqliteDatabase.getPrimaryColumnNames('orders') == ['line', 'id']


def test_table_existence_is_case_insensitive_on_sqlite(sqliteDatabase):
    assert sqliteDatabase.tableExists('CUSTOMERS')
    assert not sqliteDatabase.tableExists('suppliers')


def test_read_table_carries_its_foreign_keys(sqliteDatabase):
    definition = readTable(sqliteDatabase, 'orders', sqliteDatabase.getForeignKeys())

    assert definition.primaryKey == ['line', 'id']
    assert [foreignKey.referencedTable for foreignKey in definition.foreignKeys] == ['customers']


def test_read_table_rejects_a_missing_table(sqliteDatabase):
    with pytest.raises(SchemaError, match='not found'):
        readTable(sqliteDatabase, 'suppliers', [])


def test_generated_sqlite_ddl_round_trips(sqliteDatabase, tmp_path):
    definitions = [readTable(sqliteDatabase, name, sqliteDatabase.getForeignKeys()) for name in ('orders', 'customers')]

    with Database(connectionSettings=DatabaseConnectionConfig(type=SQLITE, database=str(tmp_path / 'copy.db'))) as copy:
        for statement in createStatements(SQLITE, SQLITE, definitions):
            copy.alter(statement.sql)

        assert copy.getPrimaryColumnNames('orders') == ['line', 'id']
        assert copy.getForeignKeys() == sqliteDatabase.getForeignKeys()
        assert [definition.name for definition in copy.getColumnDefinitions('customers')] == ['id', 'email', 'balance', 'active', 'note']


def test_clear_tables_empties_children_first(sqliteDatabase):
    sqliteDatabase.alter('PRAGMA foreign_keys = ON')
    sqliteDatabase.insert(table='customers', data=[(1, 'a@b.c', 1, 1, None)])
    sqliteDatabase.insert(table='orders', data=[(1, 1, 1), (2, 1, 1)])

    assert clearTables(sqliteDatabase, ['customers', 'orders']) == [('orders', 2), ('customers', 1)]
