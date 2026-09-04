from __future__ import annotations

from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type

from .configurationInterface import DatabaseConnectionConfig, DatabaseType
from .databaseDialects import DatabaseDialect, MSSQLDialect, MySQLDialect, OracleDialect, PostgreSQLDialect

DIALECTS: Dict[DatabaseType, DatabaseDialect] = {
    DatabaseType.MYSQL: MySQLDialect(),
    DatabaseType.ORACLE: OracleDialect(),
    DatabaseType.POSTGRESQL: PostgreSQLDialect(),
    DatabaseType.MSSQL: MSSQLDialect(),
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


    def alter(self, query: str) -> None:

        self.cursor.execute(query)
        self.connection.commit()


    def truncate(self, table: str) -> None:

        query = 'TRUNCATE TABLE {}'.format(table)
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


    def _getColumnBuckets(self, table: str) -> Tuple[List[str], List[str], List[str]]:

        allColumns = self.getAllColumnNames(table=table)
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


    def insert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100) -> None:

        columns = self.getAllColumnNames(table=table)
        columnVariables = self.dialect.placeholders(len(columns))
        query = 'INSERT INTO {} ({}) VALUES ({})'.format(table, ', '.join(columns), ', '.join(columnVariables))
        self._chunkInsert(table=table, data=data, chunkSize=chunkSize, query=query)


    def upsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100) -> None:

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self._getColumnBuckets(table=table)
        query = self.dialect.upsertQuery(table=table, allColumns=allColumns, primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)
        self._chunkInsert(table=table, data=data, chunkSize=chunkSize, query=query)


    def upsertFromStage(self, targetTable: str, stageTable: str) -> None:

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self._getColumnBuckets(table=targetTable)
        query = self.dialect.upsertFromStageQuery(targetTable=targetTable, stageTable=stageTable, allColumns=allColumns,
                                                    primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)
        self.alter(query=query)


    def swap(self, targetTable: str, stageTable: str) -> None:

        tempTable = targetTable + '_tmp'

        for query in self.dialect.swapQueries(targetTable=targetTable, stageTable=stageTable, tempTable=tempTable):
            self.cursor.execute(query)

        self.connection.commit()
