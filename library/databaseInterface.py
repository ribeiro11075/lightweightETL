from __future__ import annotations

from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type

from .configurationInterface import DatabaseConnectionConfig, DatabaseType
from .databaseDialects import DatabaseDialect, MariaDBDialect, MSSQLDialect, MySQLDialect, OracleDialect, PostgreSQLDialect, SQLiteDialect

DIALECTS: Dict[DatabaseType, DatabaseDialect] = {
    DatabaseType.MYSQL: MySQLDialect(),
    DatabaseType.ORACLE: OracleDialect(),
    DatabaseType.POSTGRESQL: PostgreSQLDialect(),
    DatabaseType.MSSQL: MSSQLDialect(),
    DatabaseType.SQLITE: SQLiteDialect(),
    DatabaseType.MARIADB: MariaDBDialect(),
    }


class Database:

    def __init__(self, connectionSettings: DatabaseConnectionConfig) -> None:
        self.connectionSettings = connectionSettings
        self.type = connectionSettings.type
        self.dialect = DIALECTS[self.type]
        self.connect()


    def connect(self) -> None:

        self.connection, self.cursor = self.dialect.connect(self.connectionSettings)


    def close(self) -> None:

        self.cursor.close()
        self.connection.close()


    def __enter__(self) -> 'Database':

        return self


    def __exit__(self, excType: Optional[Type[BaseException]], excValue: Optional[BaseException], traceback: Optional[TracebackType]) -> None:

        self.close()


    def query(self, query: str) -> List[Tuple[Any, ...]]:

        self.cursor.execute(query)

        return self.cursor.fetchall()


    def getLastQueryColumnNames(self) -> List[str]:
        """Column names from cursor.description for whatever query last ran on this
        connection -- reflects exactly what that query actually returned (an
        explicit column list, a `select *`, computed/aliased expressions, ...),
        not any table's schema. Only meaningful right after query(); there's
        nothing sensible to return before any query has run.
        """

        return [row[0] for row in self.cursor.description]


    def alter(self, query: str) -> None:

        self.cursor.execute(query)
        self.connection.commit()


    def truncate(self, table: str) -> None:

        query = self.dialect.truncateQuery(table=table)
        self.cursor.execute(query)
        self.connection.commit()


    def getAllColumnTypes(self, table: str) -> List[Any]:
        """WHERE 1=0 is valid ANSI SQL across mysql/postgresql/oracle -- reads schema
        metadata via cursor.description without scanning or fetching any rows.
        """

        query = 'SELECT * FROM {} WHERE 1=0'.format(table)
        self.cursor.execute(query)

        return [row[1] for row in self.cursor.description]


    def getAllColumnNames(self, table: str) -> List[str]:
        """See getAllColumnTypes for why the query is bounded with WHERE 1=0."""

        query = 'SELECT * FROM {} WHERE 1=0'.format(table)
        self.cursor.execute(query)

        return [row[0] for row in self.cursor.description]


    def getPrimaryColumnNames(self, table: str) -> List[str]:

        query = self.dialect.primaryKeyQuery(table=table)
        self.cursor.execute(query)

        return [row[0] for row in self.cursor.fetchall()]


    def getNonPrimaryColumnNames(self, table: str) -> List[str]:

        allColumns = self.getAllColumnNames(table=table)
        primaryColumns = self.getPrimaryColumnNames(table=table)

        return [column for column in allColumns if column not in primaryColumns]


    def _getColumnBuckets(self, table: str, columns: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
        """allColumns defaults to introspecting the table, but an explicit columns
        list (e.g. DataJobConfig.targetColumns) overrides it -- primaryColumns
        always comes from the table itself, since a primary key is a property of
        the destination, not something a job config redefines.
        """

        allColumns = columns if columns is not None else self.getAllColumnNames(table=table)
        primaryColumns = self.getPrimaryColumnNames(table=table)
        nonPrimaryColumns = [column for column in allColumns if column not in primaryColumns]

        return allColumns, primaryColumns, nonPrimaryColumns


    def _chunkInsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int, query: str) -> None:

        index = 0
        numberRecords = len(data)

        while True:

            if index > numberRecords or numberRecords == 0:
                break

            self.cursor.executemany(query, data[index:index + chunkSize])
            self.connection.commit()
            index += chunkSize


    def insert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Optional[List[str]] = None) -> None:
        """columns defaults to introspecting the table (its full column list, in
        the table's own order); pass an explicit list to insert into a specific
        subset/order instead -- data's tuples must be in that same order.
        """

        resolvedColumns = columns if columns is not None else self.getAllColumnNames(table=table)
        columnVariables = self.dialect.placeholders(len(resolvedColumns))
        query = 'INSERT INTO {} ({}) VALUES ({})'.format(table, ', '.join(resolvedColumns), ', '.join(columnVariables))
        self._chunkInsert(table=table, data=data, chunkSize=chunkSize, query=query)


    def upsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Optional[List[str]] = None) -> None:

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self._getColumnBuckets(table=table, columns=columns)
        query = self.dialect.upsertQuery(table=table, allColumns=allColumns, primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)
        self._chunkInsert(table=table, data=data, chunkSize=chunkSize, query=query)


    def upsertFromStage(self, targetTable: str, stageTable: str, columns: Optional[List[str]] = None) -> None:

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self._getColumnBuckets(table=targetTable, columns=columns)
        query = self.dialect.upsertFromStageQuery(targetTable=targetTable, stageTable=stageTable, allColumns=allColumns,
                                                    primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)
        self.alter(query=query)


    def swap(self, targetTable: str, stageTable: str) -> None:

        tempTable = targetTable + '_tmp'

        for query in self.dialect.swapQueries(targetTable=targetTable, stageTable=stageTable, tempTable=tempTable):
            self.cursor.execute(query)

        self.connection.commit()
