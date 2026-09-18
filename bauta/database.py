from __future__ import annotations

from types import TracebackType
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Type

from .configuration import WATERMARK_PLACEHOLDER, ConfigurationError, DatabaseConnectionConfig, DatabaseType
from .databaseDialects import ColumnDefinition, DatabaseDialect, ForeignKey, MariaDBDialect, MSSQLDialect, MySQLDialect, OracleDialect, PostgreSQLDialect, \
    SQLiteDialect, quoteIdentifier, splitTableName

DIALECTS: Dict[DatabaseType, DatabaseDialect] = {
    DatabaseType.MYSQL: MySQLDialect(),
    DatabaseType.ORACLE: OracleDialect(),
    DatabaseType.POSTGRESQL: PostgreSQLDialect(),
    DatabaseType.MSSQL: MSSQLDialect(),
    DatabaseType.SQLITE: SQLiteDialect(),
    DatabaseType.MARIADB: MariaDBDialect(),
    }


class RowStream:
    """The chunks of one streamed query, and the cursor they come from.

    A class rather than a generator, whose `finally` is skipped if it never
    started: close() must always release unread rows, which on MySQL would
    otherwise block the connection. Closes itself when exhausted or failing.
    """

    def __init__(self, database: 'Database', cursor: Any, chunkSize: int, firstChunk: List[Tuple[Any, ...]]) -> None:
        self._database = database
        self._cursor = cursor
        self._chunkSize = chunkSize
        self._pending: Optional[List[Tuple[Any, ...]]] = firstChunk
        self.closed = False


    def __iter__(self) -> 'RowStream':

        return self


    def __next__(self) -> List[Tuple[Any, ...]]:

        if self.closed:
            raise StopIteration

        if self._pending is not None:
            chunk, self._pending = self._pending, None
        else:
            try:
                chunk = self._cursor.fetchmany(self._chunkSize)
            except BaseException:
                self.close()
                raise

        if not chunk:
            self.close()
            raise StopIteration

        return list(chunk)


    def close(self) -> None:
        """Best-effort: the connection may already be gone, and an error here
        must not replace whatever error led to the close.
        """

        if self.closed:
            return
        self.closed = True
        self._pending = None
        self._database._streams.discard(self)

        try:
            self._database.dialect.discardRemaining(self._database.connection, self._cursor)
        except Exception:
            pass
        try:
            self._cursor.close()
        except Exception:
            pass


    def __enter__(self) -> 'RowStream':

        return self


    def __exit__(self, excType: Optional[Type[BaseException]], excValue: Optional[BaseException], traceback: Optional[TracebackType]) -> None:

        self.close()


class Database:

    def __init__(self, connectionSettings: DatabaseConnectionConfig) -> None:
        self.connectionSettings = connectionSettings
        self.type = connectionSettings.type
        self.dialect = DIALECTS[self.type]
        self.primaryKeyCache: Dict[str, List[str]] = {}
        self.columnNameCache: Dict[str, List[str]] = {}
        self._streams: Set[RowStream] = set()
        self.connect()


    def connect(self) -> None:

        self.connection, self.cursor = self.dialect.connect(self.connectionSettings)


    def close(self) -> None:
        """Closes any stream still open first, so its unread rows can't make
        closing the cursor fail. The connection is closed even if the cursor
        can't be.
        """

        for stream in list(self._streams):
            stream.close()

        try:
            self.cursor.close()
        finally:
            self.connection.close()


    def __enter__(self) -> 'Database':

        return self


    def __exit__(self, excType: Optional[Type[BaseException]], excValue: Optional[BaseException], traceback: Optional[TracebackType]) -> None:

        if excType is None:
            self.close()
            return

        # Already failing: a close error would replace the error that matters.
        try:
            self.close()
        except Exception:
            pass


    def query(self, query: str) -> List[Tuple[Any, ...]]:

        self.cursor.execute(query)

        return self.cursor.fetchall()


    def substituteWatermarkPlaceholder(self, query: str) -> str:
        """Rewrite the {{ watermark }} token into this dialect's bind placeholder,
        so the watermark is bound rather than interpolated.
        """

        return WATERMARK_PLACEHOLDER.sub(self.dialect.placeholders(1)[0], query)


    def stream(self, query: str, chunkSize: int, parameters: Optional[Sequence[Any]] = None) -> Tuple[List[str], RowStream]:
        """Runs `query` and returns (columnNames, chunks), chunks being a
        RowStream to iterate and, if it isn't read to the end, to close.

        The first chunk is fetched eagerly, since psycopg's server-side cursors
        only describe their columns once rows are fetched. Nothing here commits,
        which would invalidate such a cursor.

        With `parameters`, on the %s dialects, a literal % elsewhere in the
        query must be doubled to %%.
        """

        cursor = self.dialect.streamingCursor(self.connection, chunkSize=chunkSize)
        stream = RowStream(self, cursor, chunkSize, [])
        self._streams.add(stream)

        try:
            if parameters is None:
                cursor.execute(query)
            else:
                cursor.execute(query, tuple(parameters))

            stream._pending = list(cursor.fetchmany(chunkSize))
            columns = [row[0] for row in cursor.description]
        except BaseException:
            stream.close()
            raise

        return columns, stream


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


    def catalogColumns(self, table: str, columns: Optional[Sequence[str]] = None) -> List[str]:
        """`columns` as the catalog spells them -- or all of the table's, in its
        order -- since quoting makes names case-sensitive on Oracle and
        PostgreSQL. An exact match wins, else the one match ignoring case.
        """

        if table not in self.columnNameCache:
            self.columnNameCache[table] = self.getAllColumnNames(table=table)
        catalog = self.columnNameCache[table]

        if columns is None:
            return list(catalog)

        resolved = []
        for column in columns:
            if column in catalog:
                resolved.append(column)
                continue
            matches = [name for name in catalog if name.upper() == column.upper()]
            if not matches:
                raise ConfigurationError('{} has no column {} (it has: {})'.format(table, column, ', '.join(catalog)))
            if len(matches) > 1:
                raise ConfigurationError('{} has columns {} that differ only in case; name the one meant exactly'.format(table, ', '.join(matches)))
            resolved.append(matches[0])

        return resolved


    def quoted(self, columns: Sequence[str]) -> List[str]:

        return [quoteIdentifier(self.type, column) for column in columns]


    def getPrimaryColumnNames(self, table: str) -> List[str]:
        """The table's declared primary key, in key order, from its own schema.
        Memoized for this Database's life, one job, since upsert asks per chunk.
        """

        if table not in self.primaryKeyCache:
            self.primaryKeyCache[table] = self.dialect.primaryKey(self.cursor, table)

        return self.primaryKeyCache[table]


    def getForeignKeys(self) -> List[ForeignKey]:
        """Every foreign key in the current schema -- what subsetting follows."""

        return self.dialect.foreignKeys(self.cursor)


    def getColumnDefinitions(self, table: str) -> List[ColumnDefinition]:
        """Each column's catalog type, size and nullability -- what DDL needs."""

        return self.dialect.columnDefinitions(self.cursor, table)


    def isEncrypted(self) -> Optional[bool]:
        """Whether the server reports this connection as encrypted; None if it
        can't say, including when this login isn't allowed to ask.
        """

        try:
            return self.dialect.isEncrypted(self.cursor)
        except Exception:
            # A failed statement aborts PostgreSQL's transaction until rolled back.
            self.connection.rollback()
            return None


    def tableExists(self, table: str) -> bool:

        return self.dialect.tableExists(self.cursor, table)


    def sample(self, query: str, rows: int) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """Column names and up to `rows` rows of `query`, without reading the
        rest or needing dialect-specific LIMIT syntax.
        """

        columns, chunks = self.stream(query=query, chunkSize=rows)
        with chunks:
            firstChunk = next(chunks, [])

        return columns, list(firstChunk)


    def _getColumnBuckets(self, table: str, columns: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
        """(all, primary-key, non-primary-key) columns for an upsert. `columns`
        overrides the table's own list; the key always comes from the table.

        The split ignores case: a key column left in the non-primary bucket
        would be updated while being joined on (ORA-38104 on Oracle). A table
        without a primary key is a ConfigurationError, so it isn't retried.
        """

        allColumns = self.catalogColumns(table=table, columns=columns)
        primaryColumns = self.getPrimaryColumnNames(table=table)

        if not primaryColumns:
            raise ConfigurationError('{} has no primary key, so an upsert cannot match its rows -- add one, or use insertStrategy: swap'.format(table))

        primaryColumnsNormalized = {column.upper() for column in primaryColumns}
        nonPrimaryColumns = [column for column in allColumns if column.upper() not in primaryColumnsNormalized]

        return allColumns, primaryColumns, nonPrimaryColumns


    @staticmethod
    def _batches(data: List[Tuple[Any, ...]], chunkSize: int) -> Iterator[List[Tuple[Any, ...]]]:
        """chunkSize-sized slices, and none at all for empty data -- some
        drivers reject an executemany with no rows.
        """

        for index in range(0, len(data), chunkSize):
            yield data[index:index + chunkSize]


    def insert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Optional[List[str]] = None) -> None:
        """`columns` defaults to all of the table's, in its order; `data` must
        match. Each batch uses the dialect's bulk path where it has one, and
        commits on its own.
        """

        resolvedColumns = self.quoted(self.catalogColumns(table=table, columns=columns))
        query = 'INSERT INTO {} ({}) VALUES ({})'.format(table, ', '.join(resolvedColumns), ', '.join(self.dialect.placeholders(len(resolvedColumns))))

        for batch in self._batches(data, chunkSize):
            if not self.dialect.bulkInsert(self.cursor, table, resolvedColumns, batch):
                self.cursor.executemany(query, batch)
            self.connection.commit()


    def upsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Optional[List[str]] = None) -> None:
        """Like insert(). For the bulk path, a batch is first reduced to its
        last row per key, since one statement can't update a row twice.
        """

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self._getColumnBuckets(table=table, columns=columns)

        normalizedColumns = [column.upper() for column in allColumns]
        keyIndexes = [normalizedColumns.index(column.upper()) for column in primaryKeyColumns if column.upper() in normalizedColumns]
        canCollapse = len(keyIndexes) == len(primaryKeyColumns)

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self.quoted(allColumns), self.quoted(primaryKeyColumns), self.quoted(nonPrimaryKeyColumns)
        query = self.dialect.upsertQuery(table=table, allColumns=allColumns, primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)

        for batch in self._batches(data, chunkSize):
            loaded = False
            if canCollapse:
                lastPerKey = list({tuple(row[index] for index in keyIndexes): row for row in batch}.values())
                loaded = self.dialect.bulkUpsert(self.cursor, table, allColumns, primaryKeyColumns, nonPrimaryKeyColumns, lastPerKey)
            if not loaded:
                self.cursor.executemany(query, batch)
            self.connection.commit()


    def upsertFromStage(self, targetTable: str, stageTable: str, columns: Optional[List[str]] = None) -> None:

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = (self.quoted(bucket) for bucket in self._getColumnBuckets(table=targetTable, columns=columns))
        query = self.dialect.upsertFromStageQuery(targetTable=targetTable, stageTable=stageTable, allColumns=allColumns,
                                                    primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)
        self.alter(query=query)


    def swap(self, targetTable: str, stageTable: str) -> None:
        """Exchanges the two tables by renaming, atomically everywhere but
        Oracle, through a temporary name in the stage table's schema.
        """

        stageSchema, _ = splitTableName(stageTable)
        _, targetName = splitTableName(targetTable)
        tempTable = '{}.{}_tmp'.format(stageSchema, targetName) if stageSchema else targetName + '_tmp'

        self.dialect.swap(self.cursor, targetTable=targetTable, stageTable=stageTable, tempTable=tempTable)
        self.connection.commit()
