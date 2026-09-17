from unittest.mock import MagicMock

import pytest

from understudy_data.configuration import DatabaseConnectionConfig, DatabaseType
from understudy_data.database import Database


def _mockedDatabase(dbType: DatabaseType) -> Database:

    if dbType == DatabaseType.ORACLE:
        settings = DatabaseConnectionConfig(type=dbType, user='u', password='p', database='d', host='h', port=1234, serviceName='svc')
    else:
        settings = DatabaseConnectionConfig(type=dbType, user='u', password='p', database='d', host='h', port=1234)

    database = Database.__new__(Database)
    database.connectionSettings = settings
    database.type = dbType
    from understudy_data.database import DIALECTS
    database.dialect = DIALECTS[dbType]
    database.cursor = MagicMock()
    database.connection = MagicMock()
    database.primaryKeyCache = {}
    database._streams = set()
    database.columnNameCache = {}
    database.getAllColumnNames = MagicMock(return_value=['id', 'name'])
    database.getPrimaryColumnNames = MagicMock(return_value=['id'])

    return database


@pytest.mark.parametrize('dbType', [DatabaseType.MYSQL, DatabaseType.POSTGRESQL, DatabaseType.ORACLE, DatabaseType.MSSQL, DatabaseType.SQLITE, DatabaseType.MARIADB])
def test_upsert_executes_for_every_dialect(dbType):
    """Regression check for the bug that made mysql upserts a silent no-op, and
    the UnboundLocalError that made oracle crash outright.
    """
    database = _mockedDatabase(dbType)

    database.upsert(table='people', data=[(1, 'a'), (2, 'b')], chunkSize=100)

    if dbType == DatabaseType.POSTGRESQL:
        assert database.cursor.copy_expert.call_count == 1
        database.cursor.executemany.assert_not_called()
    elif dbType == DatabaseType.MSSQL:
        assert 'VALUES (%s, %s), (%s, %s)' in database.cursor.execute.call_args[0][0]
        database.cursor.executemany.assert_not_called()
    else:
        assert database.cursor.executemany.call_count > 0
    assert database.connection.commit.call_count > 0


@pytest.mark.parametrize('dbType', [DatabaseType.MYSQL, DatabaseType.POSTGRESQL, DatabaseType.ORACLE, DatabaseType.MSSQL, DatabaseType.SQLITE, DatabaseType.MARIADB])
def test_upsert_from_stage_executes_for_every_dialect(dbType):
    database = _mockedDatabase(dbType)

    database.upsertFromStage(targetTable='people', stageTable='people_stage')

    assert database.cursor.execute.call_count > 0
    assert database.connection.commit.call_count > 0


@pytest.mark.parametrize('dbType,expectedStatementCount', [
    (DatabaseType.MYSQL, 1),
    (DatabaseType.POSTGRESQL, 2),  # the dependent-views lookup, then the renames
    (DatabaseType.ORACLE, 3),
    (DatabaseType.MSSQL, 1),
    (DatabaseType.SQLITE, 4),
    (DatabaseType.MARIADB, 1),
    ])
def test_swap_executes_the_right_number_of_statements(dbType, expectedStatementCount):
    database = _mockedDatabase(dbType)

    database.swap(targetTable='people', stageTable='people_stage')

    assert database.cursor.execute.call_count == expectedStatementCount
    assert database.connection.commit.call_count == 1


@pytest.mark.parametrize('dbType', [DatabaseType.MYSQL, DatabaseType.POSTGRESQL, DatabaseType.ORACLE, DatabaseType.MSSQL, DatabaseType.SQLITE, DatabaseType.MARIADB])
def test_get_primary_column_names_executes_a_query(dbType):
    database = _mockedDatabase(dbType)
    database.getPrimaryColumnNames = Database.getPrimaryColumnNames.__get__(database)
    database.cursor.fetchall = MagicMock(return_value=[('id',)])

    result = database.getPrimaryColumnNames(table='people')

    assert result == ['id']
    assert database.cursor.execute.call_count == 1


@pytest.mark.parametrize('dbType,expectedPlaceholder', [
    (DatabaseType.MYSQL, '%s'),
    (DatabaseType.POSTGRESQL, '%s'),
    (DatabaseType.ORACLE, ':1'),
    (DatabaseType.SQLITE, '?'),
    (DatabaseType.MARIADB, '%s'),
    ])
def test_insert_uses_the_dialects_placeholder_style(dbType, expectedPlaceholder):
    database = _mockedDatabase(dbType)

    database.insert(table='people', data=[(1, [])], chunkSize=100)  # a list, which COPY leaves to executemany

    query = database.cursor.executemany.call_args[0][0]
    assert expectedPlaceholder in query


def test_get_all_column_names_and_types_use_a_bounded_query():
    """SELECT * with no WHERE clause is an unbounded/full-table-shaped query just to
    read cursor.description -- WHERE 1=0 is the fix, and is valid across all 3 dialects.
    """
    database = _mockedDatabase(DatabaseType.MYSQL)
    database.getAllColumnNames = Database.getAllColumnNames.__get__(database)
    database.getAllColumnTypes = Database.getAllColumnTypes.__get__(database)
    database.cursor.description = [('id', 'INT'), ('name', 'VARCHAR')]

    database.getAllColumnNames(table='people')
    database.getAllColumnTypes(table='people')

    for callArgs in database.cursor.execute.call_args_list:
        assert 'WHERE 1=0' in callArgs[0][0]


@pytest.mark.parametrize('dbType', [DatabaseType.MYSQL, DatabaseType.POSTGRESQL, DatabaseType.SQLITE])
def test_an_upsert_into_a_table_without_a_primary_key_fails_rather_than_guessing(dbType):
    """With no key there's nothing to match rows on. MySQL used to insert a
    duplicate of every row on every run; the others generated invalid SQL.
    """
    from understudy_data.configuration import ConfigurationError

    database = _mockedDatabase(dbType)
    database.getPrimaryColumnNames = MagicMock(return_value=[])

    with pytest.raises(ConfigurationError, match='no primary key'):
        database.upsert(table='people', data=[(1, 'a')])

    database.cursor.executemany.assert_not_called()


@pytest.mark.parametrize('target,stage,expectedTemp', [
    ('people', 'people_stage', 'people_tmp'),
    ('sales.people', 'sales.people_stage', 'sales.people_tmp'),
    ])
def test_swap_puts_the_temporary_table_in_the_stage_tables_schema(target, stage, expectedTemp):
    database = _mockedDatabase(DatabaseType.MYSQL)
    database.dialect = MagicMock()

    database.swap(targetTable=target, stageTable=stage)

    database.dialect.swap.assert_called_once_with(database.cursor, targetTable=target, stageTable=stage, tempTable=expectedTemp)


def test_postgresql_inserts_through_copy_one_batch_at_a_time():
    database = _mockedDatabase(DatabaseType.POSTGRESQL)

    database.insert(table='people', data=[(1, 'a'), (2, 'b'), (3, 'c')], chunkSize=2, columns=['id', 'name'])

    copies = database.cursor.copy_expert.call_args_list
    assert [call.args[0] for call in copies] == ['COPY people ("id", "name") FROM STDIN'] * 2
    assert [call.args[1].getvalue() for call in copies] == ['1\ta\n2\tb\n', '3\tc\n']
    database.cursor.executemany.assert_not_called()
    assert database.connection.commit.call_count == 2


def test_a_copied_upsert_sends_only_the_last_row_of_each_key():
    """One INSERT ... ON CONFLICT can't touch a row twice; applying the rows in
    turn, as executemany does, leaves the last one -- so that one is sent.
    """
    database = _mockedDatabase(DatabaseType.POSTGRESQL)

    database.upsert(table='people', data=[(1, 'a'), (2, 'b'), (1, 'c')])

    (copy,) = database.cursor.copy_expert.call_args_list
    assert copy.args[1].getvalue() == '1\tc\n2\tb\n'
    statements = [call.args[0] for call in database.cursor.execute.call_args_list]
    assert statements[0].startswith('CREATE TEMPORARY TABLE IF NOT EXISTS understudy_upsert_')
    assert 'ON COMMIT DELETE ROWS AS SELECT "id", "name" FROM people WITH NO DATA' in statements[0]
    assert statements[1].startswith('INSERT INTO people ("id", "name") SELECT "id", "name" FROM understudy_upsert_')
    assert statements[1].endswith('ON CONFLICT("id") DO UPDATE SET "name"=excluded."name"')


@pytest.mark.parametrize('dbType,quoted', [(DatabaseType.MYSQL, '`rank`'), (DatabaseType.MSSQL, '[rank]'), (DatabaseType.ORACLE, '"RANK"')])
def test_loads_quote_column_names_as_the_catalog_spells_them(dbType, quoted):
    """A reserved word can be a column, and a configured `rank` still finds
    Oracle's RANK, since the name is resolved before it is quoted.
    """
    database = _mockedDatabase(dbType)
    database.getAllColumnNames = MagicMock(return_value=['ID', 'RANK'] if dbType == DatabaseType.ORACLE else ['id', 'rank'])

    database.insert(table='scores', data=[(1, 2)], columns=['id', 'rank'])

    (statement,) = {call.args[0] for call in database.cursor.executemany.call_args_list} | {
        call.args[0] for call in database.cursor.execute.call_args_list}
    assert quoted in statement


def test_a_configured_column_the_table_lacks_is_a_configuration_error():
    from understudy_data.configuration import ConfigurationError

    database = _mockedDatabase(DatabaseType.MYSQL)

    with pytest.raises(ConfigurationError, match='people has no column nmae'):
        database.insert(table='people', data=[(1, 'a')], columns=['id', 'nmae'])


def test_columns_differing_only_in_case_must_be_named_exactly():
    from understudy_data.configuration import ConfigurationError

    database = _mockedDatabase(DatabaseType.POSTGRESQL)
    database.getAllColumnNames = MagicMock(return_value=['id', 'Name', 'NAME'])

    with pytest.raises(ConfigurationError, match='differ only in case'):
        database.insert(table='people', data=[(1, 'a')], columns=['id', 'name'])

    database.insert(table='people', data=[(1, 'a')], columns=['id', 'Name'])


def test_chunk_insert_splits_data_into_multiple_batches():
    """5 records with chunkSize=2 should batch as [0:2], [2:4], [4:6] (3 executemany
    calls, the last a partial batch) -- covers the chunking loop itself, not just
    that a single executemany call happens.
    """
    database = _mockedDatabase(DatabaseType.MYSQL)
    data = [(1,), (2,), (3,), (4,), (5,)]

    database.insert(table='people', data=data, chunkSize=2)

    batches = [callArgs[0][1] for callArgs in database.cursor.executemany.call_args_list]
    assert batches == [[(1,), (2,)], [(3,), (4,)], [(5,)]]
    assert database.connection.commit.call_count == 3


def test_chunk_insert_does_nothing_for_empty_data():
    database = _mockedDatabase(DatabaseType.MYSQL)

    database.insert(table='people', data=[], chunkSize=100)

    database.cursor.executemany.assert_not_called()


@pytest.mark.parametrize('dbType,expectedQuery', [
    (DatabaseType.MYSQL, 'TRUNCATE TABLE people'),
    (DatabaseType.POSTGRESQL, 'TRUNCATE TABLE people'),
    (DatabaseType.ORACLE, 'TRUNCATE TABLE people'),
    (DatabaseType.MSSQL, 'TRUNCATE TABLE people'),
    (DatabaseType.MARIADB, 'TRUNCATE TABLE people'),
    (DatabaseType.SQLITE, 'DELETE FROM people'),  # SQLite has no TRUNCATE statement
    ])
def test_truncate_uses_the_dialects_truncate_query(dbType, expectedQuery):
    database = _mockedDatabase(dbType)

    database.truncate(table='people')

    assert database.cursor.execute.call_args[0][0] == expectedQuery
    assert database.connection.commit.call_count == 1


def test_context_manager_closes_on_normal_exit():
    database = _mockedDatabase(DatabaseType.MYSQL)

    with database:
        pass

    database.cursor.close.assert_called_once()
    database.connection.close.assert_called_once()


def test_context_manager_closes_even_on_exception():
    database = _mockedDatabase(DatabaseType.MYSQL)

    with pytest.raises(ValueError):
        with database:
            raise ValueError('boom')

    database.cursor.close.assert_called_once()
    database.connection.close.assert_called_once()


@pytest.mark.parametrize('rowCount,chunkSize,expectedBatchSizes', [
    (100, 100, [100]),
    (200, 100, [100, 100]),
    (250, 100, [100, 100, 50]),
    (1, 100, [1]),
    (0, 100, []),
    ])
def test_insert_batches_without_issuing_an_empty_statement(rowCount, chunkSize, expectedBatchSizes):
    """Regression check for the chunk walk running one slice past the end: when
    rowCount was an exact multiple of chunkSize the old `index > numberRecords`
    test admitted a final, always-empty executemany (100 rows at chunkSize 100
    issued two calls, the second with []), which some drivers reject. An empty
    `data` must issue no statement at all.
    """
    database = _mockedDatabase(DatabaseType.MYSQL)
    data = [(index, 'name') for index in range(rowCount)]

    database.insert(table='people', data=data, chunkSize=chunkSize)

    batchSizes = [len(call.args[1]) for call in database.cursor.executemany.call_args_list]

    assert batchSizes == expectedBatchSizes
    assert database.connection.commit.call_count == len(expectedBatchSizes)


def test_insert_covers_every_row_exactly_once_across_chunks():
    """The batches must reassemble into the original data, in order -- a wrong
    stride would still produce a plausible-looking batch count.
    """
    database = _mockedDatabase(DatabaseType.MYSQL)
    data = [(index, 'name') for index in range(250)]

    database.insert(table='people', data=data, chunkSize=100)

    submitted = [row for call in database.cursor.executemany.call_args_list for row in call.args[1]]

    assert submitted == data


def test_upsert_of_an_empty_result_set_issues_no_statement():
    database = _mockedDatabase(DatabaseType.MYSQL)

    database.upsert(table='people', data=[], chunkSize=100)

    assert database.cursor.executemany.call_count == 0
    assert database.connection.commit.call_count == 0


def _streamingDatabase(dbType=DatabaseType.MYSQL):
    database = _mockedDatabase(dbType)
    streamCursor = MagicMock()
    streamCursor.fetchmany.side_effect = [[(1,)], [(2,)], []]
    streamCursor.description = [('id',)]
    database.dialect = MagicMock(wraps=database.dialect)
    database.dialect.streamingCursor.return_value = streamCursor
    return database, streamCursor


def test_closing_a_stream_that_was_never_read_releases_its_rows():
    """A generator closed before it starts skips its `finally`, which left
    MySQL's connection refusing every later statement.
    """
    database, streamCursor = _streamingDatabase()

    columns, chunks = database.stream('SELECT id FROM t', chunkSize=1)
    chunks.close()

    assert columns == ['id']
    database.dialect.discardRemaining.assert_called_once_with(database.connection, streamCursor)
    streamCursor.close.assert_called_once()
    assert list(chunks) == []


def test_a_stream_read_to_the_end_closes_itself():
    database, streamCursor = _streamingDatabase()

    _, chunks = database.stream('SELECT id FROM t', chunkSize=1)

    assert list(chunks) == [[(1,)], [(2,)]]
    streamCursor.close.assert_called_once()
    assert database._streams == set()


def test_closing_the_database_closes_an_open_stream_first():
    database, streamCursor = _streamingDatabase()
    order = []
    streamCursor.close.side_effect = lambda: order.append('stream')
    database.cursor.close.side_effect = lambda: order.append('cursor')

    database.stream('SELECT id FROM t', chunkSize=1)
    database.close()

    assert order == ['stream', 'cursor']
    database.connection.close.assert_called_once()


def test_a_failure_to_close_does_not_replace_the_error_that_ended_the_block():
    database = _mockedDatabase(DatabaseType.MYSQL)
    database.cursor.close.side_effect = RuntimeError('Unread result found')

    with pytest.raises(ValueError, match='the policy does not cover email'):
        with database:
            raise ValueError('the policy does not cover email')

    database.connection.close.assert_called_once()


def test_a_query_that_fails_closes_its_stream():
    database, streamCursor = _streamingDatabase()
    streamCursor.execute.side_effect = RuntimeError('syntax')

    with pytest.raises(RuntimeError):
        database.stream('SELEC id FROM t', chunkSize=1)

    streamCursor.close.assert_called_once()
    assert database._streams == set()
