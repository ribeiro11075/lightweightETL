from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, List, Tuple

from .configurationInterface import DatabaseConnectionConfig


def _mergeUpdateInsertClause(targetAlias: str, sourceAlias: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:
    """The shared ON/WHEN MATCHED/WHEN NOT MATCHED tail of a MERGE statement, used
    by both OracleDialect and MSSQLDialect.

    WHEN MATCHED is omitted entirely when there are no non-primary-key columns,
    since an empty UPDATE SET is invalid syntax on both -- a table of only
    primary-key columns has nothing to match/update against, so insert-only.
    """

    onClause = ' AND '.join('{}.{} = {}.{}'.format(targetAlias, column, sourceAlias, column) for column in primaryKeyColumns)
    insertColumns = ', '.join(allColumns)
    insertValues = ', '.join('{}.{}'.format(sourceAlias, column) for column in allColumns)

    whenMatched = ''
    if nonPrimaryKeyColumns:
        updateClause = ', '.join('{}.{} = {}.{}'.format(targetAlias, column, sourceAlias, column) for column in nonPrimaryKeyColumns)
        whenMatched = 'WHEN MATCHED THEN UPDATE SET {} '.format(updateClause)

    return 'ON ({}) {}WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})'.format(onClause, whenMatched, insertColumns, insertValues)


class DatabaseDialect(ABC):
    """Everything that differs between database types lives here, not in Database."""

    @abstractmethod
    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:
        """Returns (connection, cursor).

        Implementations import their driver lazily, inside this method, so that
        `import library` doesn't require every database driver to be installed --
        only the one you actually connect with.
        """

    @abstractmethod
    def placeholders(self, count: int) -> List[str]:
        """Parameter placeholder markers, one per bound value, in this dialect's paramstyle."""

    @abstractmethod
    def primaryKeyQuery(self, table: str) -> str:
        ...

    @abstractmethod
    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:
        ...

    @abstractmethod
    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:
        ...

    @abstractmethod
    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """One or more statements to execute in order, then commit once."""


class MySQLDialect(DatabaseDialect):

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import mysql.connector

        connection = mysql.connector.connect(user=settings.user, password=settings.password, host=settings.host, database=settings.database, port=settings.port)
        cursor = connection.cursor(buffered=True)

        return connection, cursor


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


    def primaryKeyQuery(self, table: str) -> str:

        return "SELECT k.COLUMN_NAME FROM information_schema.table_constraints t LEFT JOIN information_schema.key_column_usage k USING(constraint_name, table_schema, table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_name='{}'".format(table)


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        allColumnVariables = self.placeholders(len(allColumns))
        nonPrimaryKeyColumnVariables = [column + '=VALUES(' + column + ')' for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables), ', '.join(nonPrimaryKeyColumnVariables))


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        nonPrimaryKeyColumnVariables = [column + '=VALUES(' + column + ')' for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) SELECT {} FROM {} ON DUPLICATE KEY UPDATE {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable, ', '.join(nonPrimaryKeyColumnVariables))


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:

        return ['RENAME TABLE {} TO {}, {} TO {}, {} TO {}'.format(stageTable, tempTable, targetTable, stageTable, tempTable, targetTable)]


class PostgreSQLDialect(DatabaseDialect):

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import psycopg2

        connection = psycopg2.connect(user=settings.user, password=settings.password, host=settings.host, database=settings.database, port=settings.port)
        cursor = connection.cursor()

        return connection, cursor


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


    def primaryKeyQuery(self, table: str) -> str:

        return "SELECT c.column_name FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name=c.constraint_name WHERE t.table_name='{}' AND t.constraint_type in ('PRIMARY KEY', 'UNIQUE')".format(table)


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        allColumnVariables = self.placeholders(len(allColumns))
        nonPrimaryKeyColumnVariables = [column + '=EXCLUDED.' + column for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables), ','.join(primaryKeyColumns), ', '.join(nonPrimaryKeyColumnVariables))


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        nonPrimaryKeyColumnVariables = [column + '=EXCLUDED.' + column for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT({}) DO UPDATE SET {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable, ','.join(primaryKeyColumns), ', '.join(nonPrimaryKeyColumnVariables))


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:

        return ['ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}'.format(stageTable, tempTable, targetTable, stageTable, tempTable, targetTable)]


class OracleDialect(DatabaseDialect):

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import oracledb

        connection = oracledb.connect(user=settings.user, password=settings.password, host=settings.host, port=settings.port,
                                       service_name=settings.serviceName, sid=settings.sid)
        cursor = connection.cursor()

        return connection, cursor


    def placeholders(self, count: int) -> List[str]:

        return [':{}'.format(i + 1) for i in range(count)]


    def primaryKeyQuery(self, table: str) -> str:

        return ("SELECT cols.column_name FROM all_constraints cons JOIN all_cons_columns cols "
                "ON cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner "
                "WHERE cons.constraint_type IN ('P', 'U') AND UPPER(cols.table_name) = UPPER('{}')").format(table)


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        bindColumns = ', '.join('{} {}'.format(placeholder, column) for placeholder, column in zip(self.placeholders(len(allColumns)), allColumns))
        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        return 'MERGE INTO {} target USING (SELECT {} FROM dual) source {}'.format(table, bindColumns, mergeClause)


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        return 'MERGE INTO {} target USING {} source {}'.format(targetTable, stageTable, mergeClause)


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """Three separate statements: Oracle's cursor.execute() runs exactly one
        statement (an inherent limitation of Oracle's own SQL engine, not specific
        to any particular Python driver), unlike mysql's single multi-target RENAME
        TABLE or postgres's semicolon-chained simple-query execution.
        """

        return [
            'ALTER TABLE {} RENAME TO {}'.format(stageTable, tempTable),
            'ALTER TABLE {} RENAME TO {}'.format(targetTable, stageTable),
            'ALTER TABLE {} RENAME TO {}'.format(tempTable, targetTable),
            ]


class MSSQLDialect(DatabaseDialect):

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import pymssql

        # pymssql's port kwarg is a str, and unlike the other three drivers it
        # doesn't fall back to its own default ('1433') when explicitly passed
        # None -- omit it entirely rather than pass a broken value through
        if settings.port is not None:
            connection = pymssql.connect(server=settings.host, port=str(settings.port), user=settings.user, password=settings.password, database=settings.database)
        else:
            connection = pymssql.connect(server=settings.host, user=settings.user, password=settings.password, database=settings.database)
        cursor = connection.cursor()

        return connection, cursor


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


    def primaryKeyQuery(self, table: str) -> str:

        return ("SELECT k.COLUMN_NAME FROM information_schema.table_constraints t "
                "JOIN information_schema.key_column_usage k ON t.constraint_name = k.constraint_name AND t.table_name = k.table_name "
                "WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_name = '{}'").format(table)


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        bindColumns = ', '.join(self.placeholders(len(allColumns)))
        columnNames = ', '.join(allColumns)
        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        # MERGE requires a terminating semicolon in T-SQL, unlike Oracle
        return 'MERGE INTO {} AS target USING (VALUES ({})) AS source ({}) {};'.format(table, bindColumns, columnNames, mergeClause)


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        return 'MERGE INTO {} AS target USING {} AS source {};'.format(targetTable, stageTable, mergeClause)


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """sp_rename is a stored procedure, not DDL -- EXEC calls chain fine in one
        execute(), so (unlike Oracle) this doesn't need three separate statements.
        """

        return ["EXEC sp_rename '{0}', '{2}'; EXEC sp_rename '{1}', '{0}'; EXEC sp_rename '{2}', '{1}';".format(stageTable, targetTable, tempTable)]
