from library.databaseDialects import MySQLDialect, OracleDialect, PostgreSQLDialect

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
    """cx_Oracle's cursor.execute() can only run one statement at a time."""
    queries = OracleDialect().swapQueries('people', 'people_stage', 'people_tmp')
    assert queries == [
        'ALTER TABLE people_stage RENAME TO people_tmp',
        'ALTER TABLE people RENAME TO people_stage',
        'ALTER TABLE people_tmp RENAME TO people',
        ]
