from library.databaseDialects import ColumnCategory, MariaDBDialect, MSSQLDialect, MySQLDialect, OracleDialect, PostgreSQLDialect, SQLiteDialect

ALL_COLUMNS = ['id', 'name', 'amount']
PRIMARY_KEY_COLUMNS = ['id']
NON_PRIMARY_KEY_COLUMNS = ['name', 'amount']


def test_mysql_placeholders():
    assert MySQLDialect().placeholders(3) == ['%s', '%s', '%s']


def test_mysql_primary_key_query_names_the_table():
    query = MySQLDialect().primaryKeyQuery('people')
    assert "t.table_name='people'" in query
    assert "t.constraint_type='PRIMARY KEY'" in query


def test_mysql_upsert_query():
    query = MySQLDialect().upsertQuery('people', ALL_COLUMNS, PRIMARY_KEY_COLUMNS, NON_PRIMARY_KEY_COLUMNS)
    assert query == ('INSERT INTO people (id, name, amount) VALUES (%s, %s, %s) '
                      'ON DUPLICATE KEY UPDATE name=VALUES(name), amount=VALUES(amount)')


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
                      'ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name, amount=EXCLUDED.amount')


def test_postgresql_swap_is_one_chained_statement():
    queries = PostgreSQLDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert len(queries) == 1
    assert queries[0].count(';') == 2


def test_oracle_placeholders_are_positional_binds():
    assert OracleDialect().placeholders(3) == [':1', ':2', ':3']


def test_oracle_primary_key_query_names_the_table_case_insensitively():
    query = OracleDialect().primaryKeyQuery('people')
    assert "UPPER(cols.table_name) = UPPER('people')" in query
    assert "constraint_type IN ('P', 'U')" in query


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


def test_mssql_primary_key_query_names_the_table():
    query = MSSQLDialect().primaryKeyQuery('people')
    assert "t.table_name = 'people'" in query
    assert "t.constraint_type = 'PRIMARY KEY'" in query


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
    assert MariaDBDialect().primaryKeyQuery('people') == MySQLDialect().primaryKeyQuery('people')
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


def test_sqlite_primary_key_query_uses_the_pragma_table_valued_function():
    query = SQLiteDialect().primaryKeyQuery('people')
    assert query == "SELECT name FROM pragma_table_info('people') WHERE pk > 0 ORDER BY pk"


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


def test_sqlite_swap_is_three_separate_statements():
    """Same reasoning as the equivalent Oracle test: sqlite3's cursor.execute() can
    only run one statement at a time.
    """
    queries = SQLiteDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert queries == [
        'ALTER TABLE people_stage RENAME TO people_tmp',
        'ALTER TABLE people RENAME TO people_stage',
        'ALTER TABLE people_tmp RENAME TO people',
        ]
