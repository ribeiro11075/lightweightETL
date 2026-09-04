from unittest.mock import MagicMock

import pytest

from library.configurationInterface import DatabaseConnectionConfig, DatabaseType
from library.databaseInterface import Database


def _mockedDatabase(dbType: DatabaseType) -> Database:

    if dbType == DatabaseType.ORACLE:
        settings = DatabaseConnectionConfig(type=dbType, user='u', password='p', database='d', host='h', port=1234, serviceName='svc')
    else:
        settings = DatabaseConnectionConfig(type=dbType, user='u', password='p', database='d', host='h', port=1234)

    database = Database.__new__(Database)
    database.connectionSettings = settings
    database.type = dbType
    from library.databaseInterface import DIALECTS
    database.dialect = DIALECTS[dbType]
    database.cursor = MagicMock()
    database.connection = MagicMock()
    database.getAllColumnNames = MagicMock(return_value=['id', 'name'])
    database.getPrimaryColumnNames = MagicMock(return_value=['id'])

    return database


@pytest.mark.parametrize('dbType', [DatabaseType.MYSQL, DatabaseType.POSTGRESQL, DatabaseType.ORACLE])
def test_upsert_executes_for_every_dialect(dbType):
    """Regression check for the bug that made mysql upserts a silent no-op, and
    the UnboundLocalError that made oracle crash outright.
    """
    database = _mockedDatabase(dbType)

    database.upsert(table='people', data=[(1, 'a'), (2, 'b')], chunkSize=100)

    assert database.cursor.executemany.call_count > 0
    assert database.connection.commit.call_count > 0


@pytest.mark.parametrize('dbType', [DatabaseType.MYSQL, DatabaseType.POSTGRESQL, DatabaseType.ORACLE])
def test_upsert_from_stage_executes_for_every_dialect(dbType):
    database = _mockedDatabase(dbType)

    database.upsertFromStage(targetTable='people', stageTable='people_stage')

    assert database.cursor.execute.call_count > 0
    assert database.connection.commit.call_count > 0


@pytest.mark.parametrize('dbType,expectedStatementCount', [
    (DatabaseType.MYSQL, 1),
    (DatabaseType.POSTGRESQL, 1),
    (DatabaseType.ORACLE, 3),
    ])
def test_swap_executes_the_right_number_of_statements(dbType, expectedStatementCount):
    database = _mockedDatabase(dbType)

    database.swap(targetTable='people', stageTable='people_stage')

    assert database.cursor.execute.call_count == expectedStatementCount
    assert database.connection.commit.call_count == 1


@pytest.mark.parametrize('dbType', [DatabaseType.MYSQL, DatabaseType.POSTGRESQL, DatabaseType.ORACLE])
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
    ])
def test_insert_uses_the_dialects_placeholder_style(dbType, expectedPlaceholder):
    database = _mockedDatabase(dbType)

    database.insert(table='people', data=[(1, 'a')], chunkSize=100)

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
