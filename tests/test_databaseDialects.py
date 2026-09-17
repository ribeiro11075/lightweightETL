import pytest

from lightweight_etl.databaseDialects import ColumnCategory, MariaDBDialect, MSSQLDialect, MySQLDialect, OracleDialect, PostgreSQLDialect, SQLiteDialect

ALL_COLUMNS = ['id', 'name', 'amount']
PRIMARY_KEY_COLUMNS = ['id']
NON_PRIMARY_KEY_COLUMNS = ['name', 'amount']


def test_mysql_placeholders():
    assert MySQLDialect().placeholders(3) == ['%s', '%s', '%s']


@pytest.mark.parametrize('dialect', [MySQLDialect(), PostgreSQLDialect(), OracleDialect(), MSSQLDialect()])
@pytest.mark.parametrize('query', ['primaryKeyQuery', 'columnsQuery', 'tableExistsQuery'])
def test_catalog_queries_bind_the_schema_and_table_rather_than_interpolating_them(dialect, query):
    """A lookup that ignored the schema used to pick up a same-named table
    elsewhere on the server, and its key columns with it. Both parts are bound,
    and a NULL schema falls back to the connection's current one.
    """
    text = getattr(dialect, query)()

    assert text.count('{}') == 2
    assert 'COALESCE(' in text


@pytest.mark.parametrize('dialect', [MySQLDialect(), PostgreSQLDialect(), OracleDialect(), MSSQLDialect()])
def test_the_primary_key_query_ignores_unique_constraints(dialect):
    """Treating UNIQUE columns as key columns made an upsert match on (id,
    email): a changed email became an insert that violated the real key, and
    PostgreSQL refused the ON CONFLICT list outright.
    """
    assert 'UNIQUE' not in dialect.primaryKeyQuery().upper().replace('INDISUNIQUE', '')
    assert "'U'" not in dialect.primaryKeyQuery()


class _RecordingCursor:

    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, query, parameters=()):
        self.executed.append((query, parameters))

    def fetchall(self):
        return self.rows


@pytest.mark.parametrize('table,expected', [('people', (None, 'people')), ('sales.people', ('sales', 'people'))])
def test_the_primary_key_lookup_splits_a_qualified_table_name(table, expected):
    cursor = _RecordingCursor([('id',)])

    assert MySQLDialect().primaryKey(cursor, table) == ['id']
    assert cursor.executed[0][1] == expected
    assert '%s' in cursor.executed[0][0]


def test_mysql_upsert_query():
    query = MySQLDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query == ('INSERT INTO people (id, name, amount) VALUES (%s, %s, %s) '
                      'ON DUPLICATE KEY UPDATE name=VALUES(name), amount=VALUES(amount)')


def test_mysql_upsert_of_a_key_only_table_does_not_use_insert_ignore():
    """INSERT IGNORE also turns truncation, NOT NULL and foreign-key errors into
    warnings, silently dropping or mangling rows.
    """
    query = MySQLDialect().upsertQuery('links', ['a', 'b'], ['a', 'b'], [])

    assert 'IGNORE' not in query
    assert query.endswith('ON DUPLICATE KEY UPDATE links.a=links.a')


def test_mysql_upsert_from_stage_query():
    query = MySQLDialect().upsertFromStageQuery('people', 'people_stage', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query == ('INSERT INTO people (id, name, amount) SELECT id, name, amount FROM people_stage '
                      'ON DUPLICATE KEY UPDATE name=VALUES(name), amount=VALUES(amount)')


def test_mysql_swap_is_one_statement():
    queries = MySQLDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert queries == ['RENAME TABLE people_stage TO people_tmp, people TO people_stage, people_tmp TO people']


def test_postgresql_placeholders():
    assert PostgreSQLDialect().placeholders(2) == ['%s', '%s']


def test_postgresql_upsert_query():
    query = PostgreSQLDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query == ('INSERT INTO people (id, name, amount) VALUES (%s, %s, %s) '
                      'ON CONFLICT(id) DO UPDATE SET name=excluded.name, amount=excluded.amount')


def test_postgresql_swap_is_one_chained_statement():
    queries = PostgreSQLDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert len(queries) == 1
    assert queries[0].count(';') == 2


@pytest.mark.parametrize('dialect', [PostgreSQLDialect(), OracleDialect(), SQLiteDialect()])
def test_a_rename_takes_the_new_name_unqualified(dialect):
    """`ALTER TABLE sales.orders RENAME TO sales.orders_tmp` is a syntax error;
    the renamed table stays in its schema anyway.
    """
    queries = ' '.join(dialect.swapQueries('sales.orders', 'sales.orders_stage', 'sales.orders_tmp'))

    assert 'RENAME TO sales.' not in queries
    assert 'ALTER TABLE sales.orders_stage RENAME TO orders_tmp' in queries
    assert 'ALTER TABLE sales.orders RENAME TO orders_stage' in queries
    assert 'ALTER TABLE sales.orders_tmp RENAME TO orders' in queries


def test_mssql_sp_rename_takes_the_new_name_unqualified():
    """sp_rename would otherwise create a table literally named `sales.orders_tmp`."""
    (query,) = MSSQLDialect().swapQueries('sales.orders', 'sales.orders_stage', 'sales.orders_tmp')

    assert query == ("EXEC sp_rename 'sales.orders_stage', 'orders_tmp'; EXEC sp_rename 'sales.orders', 'orders_stage'; "
                     "EXEC sp_rename 'sales.orders_tmp', 'orders';")


def test_oracle_placeholders_are_positional_binds():
    assert OracleDialect().placeholders(3) == [':1', ':2', ':3']


def test_oracle_catalog_queries_look_in_the_current_schema_not_every_schema():
    for query in (OracleDialect().primaryKeyQuery(), OracleDialect().columnsQuery(), OracleDialect().tableExistsQuery()):
        assert "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')" in query
        assert 'table_name = UPPER({})' in query


def test_oracle_upsert_query_is_a_merge_with_matched_and_not_matched():
    query = OracleDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query.startswith('MERGE INTO people target USING (SELECT :1 id, :2 name, :3 amount FROM dual) source')
    assert 'ON (target.id = source.id)' in query
    assert 'WHEN MATCHED THEN UPDATE SET target.name = source.name, target.amount = source.amount' in query
    assert 'WHEN NOT MATCHED THEN INSERT (id, name, amount) VALUES (source.id, source.name, source.amount)' in query


def test_oracle_upsert_query_omits_when_matched_with_no_non_primary_columns():
    """An empty UPDATE SET is invalid Oracle syntax -- a table of only primary-key
    columns has nothing to update, so WHEN MATCHED must be dropped entirely.
    """
    query = OracleDialect().upsertQuery('ids_only', ['id'], ['id'], [])
    assert 'WHEN MATCHED' not in query
    assert 'WHEN NOT MATCHED THEN INSERT (id) VALUES (source.id)' in query


def test_oracle_upsert_from_stage_query_sources_the_stage_table_not_dual():
    query = OracleDialect().upsertFromStageQuery('people', 'people_stage', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query.startswith('MERGE INTO people target USING people_stage source')
    assert 'FROM dual' not in query


def test_oracle_swap_is_three_separate_statements():
    """Oracle's cursor.execute() can only run one statement at a time."""
    queries = OracleDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert queries == [
        'ALTER TABLE people_stage RENAME TO people_tmp',
        'ALTER TABLE people RENAME TO people_stage',
        'ALTER TABLE people_tmp RENAME TO people',
        ]


def test_mssql_placeholders():
    assert MSSQLDialect().placeholders(3) == ['%s', '%s', '%s']


def test_mssql_upsert_query_is_a_merge_with_matched_and_not_matched():
    query = MSSQLDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query.startswith('MERGE INTO people AS target USING (VALUES (%s, %s, %s)) AS source (id, name, amount)')
    assert 'ON (target.id = source.id)' in query
    assert 'WHEN MATCHED THEN UPDATE SET target.name = source.name, target.amount = source.amount' in query
    assert 'WHEN NOT MATCHED THEN INSERT (id, name, amount) VALUES (source.id, source.name, source.amount)' in query
    assert query.endswith(';')  # MERGE requires a terminating semicolon in T-SQL


def test_mssql_upsert_query_omits_when_matched_with_no_non_primary_columns():
    """Same reasoning as the equivalent Oracle test: an empty UPDATE SET is invalid
    T-SQL syntax too, so WHEN MATCHED must be dropped entirely.
    """
    query = MSSQLDialect().upsertQuery('ids_only', ['id'], ['id'], [])
    assert 'WHEN MATCHED' not in query
    assert 'WHEN NOT MATCHED THEN INSERT (id) VALUES (source.id)' in query


def test_mssql_upsert_from_stage_query_sources_the_stage_table_not_values():
    query = MSSQLDialect().upsertFromStageQuery('people', 'people_stage', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query.startswith('MERGE INTO people AS target USING people_stage AS source')
    assert 'VALUES' not in query.split('ON')[0]


def test_mssql_swap_is_one_statement_of_chained_sp_rename_calls():
    """sp_rename is a stored procedure, not DDL -- chaining three EXEC calls in one
    execute() works, unlike Oracle's ALTER TABLE RENAME which needs three separate
    execute() calls.
    """
    queries = MSSQLDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert queries == ["EXEC sp_rename 'people_stage', 'people_tmp'; EXEC sp_rename 'people', 'people_stage'; EXEC sp_rename 'people_tmp', 'people';"]


def test_mariadb_reuses_mysql_dialect_wholesale():
    """MariaDBDialect adds nothing of its own -- proves it inherits every query/
    placeholder method from MySQLDialect unchanged.
    """
    assert MariaDBDialect().placeholders(2) == MySQLDialect().placeholders(2)
    assert MariaDBDialect().primaryKeyQuery() == MySQLDialect().primaryKeyQuery()
    assert MariaDBDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS) == \
        MySQLDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert MariaDBDialect().swapQueries('people', 'people_stage', 'people_tmp') == MySQLDialect().swapQueries('people', 'people_stage', 'people_tmp')


def test_sqlite_placeholders():
    assert SQLiteDialect().placeholders(3) == ['?', '?', '?']


def test_sqlite_truncate_query_is_a_delete_since_sqlite_has_no_truncate():
    assert SQLiteDialect().truncateQuery('people') == 'DELETE FROM people'


def test_other_dialects_default_truncate_query_is_ansi_truncate():
    assert MySQLDialect().truncateQuery('people') == 'TRUNCATE TABLE people'
    assert PostgreSQLDialect().truncateQuery('people') == 'TRUNCATE TABLE people'
    assert OracleDialect().truncateQuery('people') == 'TRUNCATE TABLE people'
    assert MSSQLDialect().truncateQuery('people') == 'TRUNCATE TABLE people'


@pytest.mark.parametrize('table,expected', [('people', ('people', 'main')), ('other.people', ('people', 'other'))])
def test_sqlite_primary_key_lookup_binds_the_table_and_attached_database(table, expected):
    cursor = _RecordingCursor([('id',)])

    assert SQLiteDialect().primaryKey(cursor, table) == ['id']
    assert 'pragma_table_info(?, ?)' in cursor.executed[0][0]
    assert cursor.executed[0][1] == expected


def test_sqlite_upsert_query():
    query = SQLiteDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query == ('INSERT INTO people (id, name, amount) VALUES (?, ?, ?) '
                      'ON CONFLICT(id) DO UPDATE SET name=excluded.name, amount=excluded.amount')


def test_sqlite_upsert_from_stage_query():
    """The "WHERE true" is required -- SQLite rejects INSERT ... SELECT ... ON
    CONFLICT outright as a grammar ambiguity without some clause disambiguating
    the SELECT first; test_integration_sqlite.py's test_upsert_from_stage is what
    actually caught this against a real SQLite engine.
    """
    query = SQLiteDialect().upsertFromStageQuery('people', 'people_stage', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query == ('INSERT INTO people (id, name, amount) SELECT id, name, amount FROM people_stage WHERE true '
                      'ON CONFLICT(id) DO UPDATE SET name=excluded.name, amount=excluded.amount')


def test_mysql_column_category_maps_known_type_names():
    assert MySQLDialect().columnCategory('INT') == ColumnCategory.NUMBER
    assert MySQLDialect().columnCategory('varchar') == ColumnCategory.TEXT  # case-insensitive
    assert MySQLDialect().columnCategory('DATETIME') == ColumnCategory.DATE
    assert MySQLDialect().columnCategory('BLOB') is None
    assert MySQLDialect().columnCategory(1234) is None  # non-string dataType is simply unrecognized, not an error


def test_mariadb_column_category_is_inherited_from_mysql():
    assert MariaDBDialect().columnCategory('INT') == ColumnCategory.NUMBER


def test_postgresql_column_category_maps_known_oids():
    assert PostgreSQLDialect().columnCategory(23) == ColumnCategory.NUMBER  # int4
    assert PostgreSQLDialect().columnCategory(25) == ColumnCategory.TEXT  # text
    assert PostgreSQLDialect().columnCategory(1114) == ColumnCategory.DATE  # timestamp
    assert PostgreSQLDialect().columnCategory(9999) is None


def test_oracle_column_category_matches_on_db_type_name():
    class _FakeDbType:
        def __init__(self, name):
            self.name = name

    assert OracleDialect().columnCategory(_FakeDbType('DB_TYPE_NUMBER')) == ColumnCategory.NUMBER
    assert OracleDialect().columnCategory(_FakeDbType('DB_TYPE_VARCHAR')) == ColumnCategory.TEXT
    assert OracleDialect().columnCategory(_FakeDbType('DB_TYPE_TIMESTAMP')) == ColumnCategory.DATE
    assert OracleDialect().columnCategory(_FakeDbType('DB_TYPE_BLOB')) is None
    assert OracleDialect().columnCategory('not a db type object') is None


def test_mssql_and_sqlite_column_category_is_unimplemented():
    """Neither dialect overrides columnCategory -- MSSQL because pymssql's type
    codes aren't reliably introspectable without importing the driver, SQLite
    because sqlite3 never reports a real type at all. Both fall back to the base
    class's None, same as any dialect handed a type it doesn't recognize.
    """
    assert MSSQLDialect().columnCategory('anything') is None
    assert SQLiteDialect().columnCategory(None) is None


def test_sqlite_swap_is_three_separate_statements_in_one_transaction():
    """sqlite3's cursor.execute() runs one statement at a time, and doesn't open a
    transaction before DDL -- without the BEGIN, each rename would commit alone.
    """
    queries = SQLiteDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert queries == [
        'BEGIN',
        'ALTER TABLE people_stage RENAME TO people_tmp',
        'ALTER TABLE people RENAME TO people_stage',
        'ALTER TABLE people_tmp RENAME TO people',
        ]


@pytest.mark.parametrize('dialect', [
    MySQLDialect(), MariaDBDialect(), PostgreSQLDialect(), SQLiteDialect(), OracleDialect(), MSSQLDialect(),
    ])
def test_upsert_of_a_key_only_table_is_valid_sql(dialect):
    """Every column is part of the primary key, so there is nothing to update on
    a conflict. An empty SET clause is a syntax error, and the INSERT-based
    dialects used to emit a dangling `DO UPDATE SET` / `ON DUPLICATE KEY UPDATE`
    and fail at the database (sqlite3: "incomplete input"). Oracle and MSSQL
    already dropped WHEN MATCHED from their MERGE for this case.

    Bridge tables and id-only lookup tables have exactly this shape. MySQL
    assigns the key to itself, the one conflict action it has that does nothing.
    """
    query = dialect.upsertQuery(table='t', allColumns=['id'], primaryKeyColumns=['id'], nonPrimaryKeyColumns=[])

    assert not query.rstrip().endswith(('SET', 'UPDATE'))
    assert 'DO UPDATE SET ' not in query
    assert 'ON DUPLICATE KEY UPDATE ' not in query or query.endswith('ON DUPLICATE KEY UPDATE t.id=t.id')


@pytest.mark.parametrize('dialect', [
    MySQLDialect(), MariaDBDialect(), PostgreSQLDialect(), SQLiteDialect(), OracleDialect(), MSSQLDialect(),
    ])
def test_upsert_from_stage_of_a_key_only_table_is_valid_sql(dialect):
    query = dialect.upsertFromStageQuery(targetTable='t', stageTable='s', allColumns=['id'], primaryKeyColumns=['id'], nonPrimaryKeyColumns=[])

    assert not query.rstrip().endswith(('SET', 'UPDATE'))
    assert 'DO UPDATE SET ' not in query
    assert 'ON DUPLICATE KEY UPDATE ' not in query or query.endswith('ON DUPLICATE KEY UPDATE t.id=t.id')


def test_copy_text_escapes_what_the_text_format_treats_specially():
    import datetime
    import decimal
    from lightweight_etl.databaseDialects import _copyText

    stream = _copyText([
        (None, 'a\tb\nc\\d\re', True, False, decimal.Decimal('1.50'), float('nan'), float('-inf'), 2.5),
        (datetime.datetime(2026, 1, 2, 3, 4, 5), datetime.date(2026, 1, 2), datetime.time(3, 4), b'\x00\xff', memoryview(b'\x01'), 7, '', '\\N'),
        ])

    assert stream.getvalue() == (
        '\\N\ta\\tb\\nc\\\\d\\re\tt\tf\t1.50\tNaN\t-Infinity\t2.5\n'
        '2026-01-02 03:04:05\t2026-01-02\t03:04:00\t\\\\x00ff\t\\\\x01\t7\t\t\\\\N\n')


def test_copy_text_gives_up_on_a_value_it_cannot_spell():
    import datetime
    from lightweight_etl.databaseDialects import _copyText

    assert _copyText([(1, [1, 2])]) is None
    assert _copyText([(1, datetime.timedelta(days=1))]) is None


class _ExecutingCursor:

    def __init__(self):
        self.executed = []

    def execute(self, query, parameters=()):
        self.executed.append((query, parameters))


def test_mssql_bulk_insert_sends_a_thousand_rows_per_statement():
    cursor = _ExecutingCursor()
    rows = [(index, 'n{}'.format(index)) for index in range(2500)]

    assert MSSQLDialect().bulkInsert(cursor, 'people', ['id', 'name'], rows) is True

    assert [query.count('(%s, %s)') for query, _ in cursor.executed] == [1000, 1000, 500]
    assert cursor.executed[0][0].startswith('INSERT INTO people (id, name) VALUES (%s, %s), (%s, %s)')
    assert cursor.executed[2][1] == tuple(value for row in rows[2000:] for value in row)


def test_mssql_bulk_upsert_merges_many_rows_per_statement():
    cursor = _ExecutingCursor()

    assert MSSQLDialect().bulkUpsert(cursor, 'people', ['id', 'name'], ['id'], ['name'], [(1, 'a'), (2, 'b')]) is True

    ((query, parameters),) = cursor.executed
    assert query.startswith('MERGE INTO people AS target USING (VALUES (%s, %s), (%s, %s)) AS source (id, name) ON (target.id = source.id)')
    assert parameters == (1, 'a', 2, 'b')


def test_only_postgresql_and_mssql_have_a_bulk_path():
    for dialect in (MySQLDialect(), MariaDBDialect(), OracleDialect(), SQLiteDialect()):
        assert dialect.bulkInsert(None, 't', ['id'], [(1,)]) is False
        assert dialect.bulkUpsert(None, 't', ['id'], ['id'], [], [(1,)]) is False


def _settings(**overrides):
    from lightweight_etl.configuration import DatabaseConnectionConfig

    fields = dict(type='postgresql', user='u', password='secret', database='d', host='h', port=5432)
    fields.update(overrides)
    return DatabaseConnectionConfig(**fields)


def test_connect_arguments_add_the_options_to_the_fields():
    arguments = PostgreSQLDialect().connectArguments(_settings(options={'sslmode': 'verify-full', 'sslrootcert': '/ca.pem'}))

    assert arguments == {'user': 'u', 'password': 'secret', 'host': 'h', 'database': 'd', 'port': 5432,
                         'sslmode': 'verify-full', 'sslrootcert': '/ca.pem'}


def test_an_option_that_duplicates_a_field_is_refused():
    from lightweight_etl.configuration import ConfigurationError

    with pytest.raises(ConfigurationError, match='options host, password duplicate'):
        PostgreSQLDialect().connectArguments(_settings(options={'host': 'elsewhere', 'password': 'other', 'sslmode': 'require'}))


def test_each_dialect_maps_the_fields_to_its_drivers_own_argument_names():
    oracle = OracleDialect().connectArguments(_settings(type='oracle', serviceName='svc', options={'protocol': 'tcps'}))
    mssql = MSSQLDialect().connectArguments(_settings(type='mssql', port=None))
    sqlite = SQLiteDialect().connectArguments(_settings(type='sqlite', database='/tmp/x.db', options={'uri': True}))

    assert oracle == {'user': 'u', 'password': 'secret', 'host': 'h', 'port': 5432, 'service_name': 'svc', 'sid': None, 'protocol': 'tcps'}
    assert mssql == {'server': 'h', 'user': 'u', 'password': 'secret', 'database': 'd'}
    assert sqlite == {'database': '/tmp/x.db', 'timeout': 30.0, 'uri': True}


def test_sqlite_cannot_tell_whether_a_connection_is_encrypted():
    assert SQLiteDialect().isEncrypted(None) is None


@pytest.mark.parametrize('dialect,row,expected', [
    (MySQLDialect(), ('Ssl_cipher', 'TLS_AES_128_GCM_SHA256'), True),
    (MySQLDialect(), ('Ssl_cipher', ''), False),
    (PostgreSQLDialect(), (True,), True),
    (PostgreSQLDialect(), None, None),
    (OracleDialect(), ('tcps',), True),
    (OracleDialect(), ('tcp',), False),
    (MSSQLDialect(), ('TRUE',), True),
    (MSSQLDialect(), ('FALSE',), False),
    ])
def test_encryption_is_read_from_what_the_server_reports(dialect, row, expected):

    class _Cursor(_RecordingCursor):
        def fetchone(self):
            return row

    assert dialect.isEncrypted(_Cursor([])) is expected
