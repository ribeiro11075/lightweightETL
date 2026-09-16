from __future__ import annotations

from types import TracebackType
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Type

from .configuration import WATERMARK_PLACEHOLDER, ConfigurationError, DatabaseConnectionConfig, DatabaseType
from .databaseDialects import ColumnDefinition, DatabaseDialect, ForeignKey, MariaDBDialect, MSSQLDialect, MySQLDialect, OracleDialect, PostgreSQLDialect, \
    SQLiteDialect, splitTableName

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
        self.primaryKeyCache: Dict[str, List[str]] = {}
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


    def substituteWatermarkPlaceholder(self, query: str) -> str:
        """Rewrite the {{ watermark }} token into this dialect's bind placeholder.

        The token exists so one sourceQuery is portable across dialects whose
        paramstyles disagree (%s for mysql/postgresql/mssql, :1 for oracle, ?
        for sqlite) -- and so the watermark arrives as a *bound value* rather
        than interpolated text, which keeps it typed by the driver and keeps a
        string watermark from being able to alter the statement.
        """

        return WATERMARK_PLACEHOLDER.sub(self.dialect.placeholders(1)[0], query)


    def stream(self, query: str, chunkSize: int, parameters: Optional[Sequence[Any]] = None) -> Tuple[List[str], Iterator[List[Tuple[Any, ...]]]]:
        """Runs `query` and returns (columnNames, chunkIterator).

        The memory ceiling of an extract becomes chunkSize * row width, whatever
        the table's size -- where query()/fetchall() builds Python objects for
        every row at once. Prefer this for anything that isn't known to be small;
        query() remains the right call for metadata.

        Two deliberate details:

        The first chunk is fetched eagerly, before returning. That's what makes
        `columnNames` trustworthy: cursor.description is only reliably populated
        once rows have actually been fetched on some drivers (notably psycopg2's
        server-side cursors), so describing off a bare execute() can hand back
        None. Fetching one bounded chunk costs nothing and removes the driver
        dependence.

        Nothing here commits. A commit would invalidate a PostgreSQL server-side
        cursor mid-iteration; the extract side has nothing to commit anyway.

        The iterator closes its cursor when it's exhausted or abandoned -- the
        finally runs on GeneratorExit too, so a transform raising part-way
        through doesn't leak a server-side cursor for the life of the connection.

        `parameters` are bound by the driver, not interpolated. One caveat comes
        with them on the %s-paramstyle dialects (mysql, postgresql, mssql): once
        a statement carries parameters, a literal % elsewhere in it (a LIKE
        '%foo%', say) is read as a format specifier and has to be doubled to %%.
        Passing no parameters leaves the query untouched, so this only applies to
        a query that actually binds something.
        """

        cursor = self.dialect.streamingCursor(self.connection, chunkSize=chunkSize)

        if parameters is None:
            cursor.execute(query)
        else:
            cursor.execute(query, tuple(parameters))

        firstChunk = cursor.fetchmany(chunkSize)
        columns = [row[0] for row in cursor.description]

        def chunks() -> Iterator[List[Tuple[Any, ...]]]:

            try:
                chunk = firstChunk
                while chunk:
                    yield chunk
                    chunk = cursor.fetchmany(chunkSize)
            finally:
                # Let the dialect release anything still queued before the close:
                # some drivers (mysql.connector) hold unread rows against the
                # connection, where closing the cursor alone leaves it unusable
                # for the next statement.
                try:
                    self.dialect.discardRemaining(self.connection, cursor)
                except Exception:
                    pass
                # Best-effort: the connection may already be gone. An abandoned
                # generator is finalized by the garbage collector, which can run
                # after Database.__exit__ has closed the connection out from
                # under it -- and a cursor-close error raised from a finally
                # during unwinding would replace whatever real exception sent us
                # here in the first place.
                try:
                    cursor.close()
                except Exception:
                    pass

        return columns, chunks()


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
        """The table's declared primary key, in key order.

        Looked up in the table's own schema -- `schema.table`, or the
        connection's current schema -- so a same-named table elsewhere can't
        contribute columns.

        Memoized for the life of this Database (which is one job). A streaming
        upsert calls this once per chunk through _getColumnBuckets, which would
        otherwise put a catalog query between every batch of rows. A table's
        primary key doesn't change under a running job, and a Database is opened
        per job and closed with it.
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


    def tableExists(self, table: str) -> bool:

        return self.dialect.tableExists(self.cursor, table)


    def sample(self, query: str, rows: int) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """Column names and up to `rows` rows of `query`, without reading the rest.

        Built on stream(), so it needs no dialect-specific LIMIT syntax: one
        bounded chunk is fetched and the stream abandoned.
        """

        columns, chunks = self.stream(query=query, chunkSize=rows)
        try:
            firstChunk = next(chunks, [])
        finally:
            chunks.close()  # type: ignore[attr-defined]

        return columns, list(firstChunk)


    def getNonPrimaryColumnNames(self, table: str) -> List[str]:

        allColumns = self.getAllColumnNames(table=table)
        primaryColumns = self.getPrimaryColumnNames(table=table)

        return [column for column in allColumns if column not in primaryColumns]


    def _getColumnBuckets(self, table: str, columns: Optional[List[str]] = None) -> Tuple[List[str], List[str], List[str]]:
        """allColumns defaults to introspecting the table, but an explicit columns
        list (e.g. DataJobConfig.targetColumns) overrides it -- primaryColumns
        always comes from the table itself, since a primary key is a property of
        the destination, not something a job config redefines.

        The primary-key split is case-insensitive because this is the one place
        that compares a *caller's* spelling of a column against the *database's*,
        and the two disagree by default: Oracle reports unquoted identifiers as
        JOB, PostgreSQL as job, MySQL as declared. A config naming `job` against
        an Oracle table would otherwise find no match, leaving the key column in
        the non-primary bucket -- which puts it in a MERGE's UPDATE SET while the
        ON clause is already joining on it (ORA-38104), and on other dialects
        silently writes the key column as if it were data.

        Only the comparison is normalized; the returned lists keep the spelling
        each side supplied, which is what the generated SQL needs. Unquoted
        identifiers are case-insensitive to every dialect here, so `job` in the
        statement still resolves to a JOB column.

        A table without a primary key can't be upserted into: there is nothing
        to match rows on, and the generated statement would either be invalid
        or, on MySQL, silently insert duplicates on every run. That's a
        ConfigurationError, so the job fails once instead of being retried.
        """

        allColumns = columns if columns is not None else self.getAllColumnNames(table=table)
        primaryColumns = self.getPrimaryColumnNames(table=table)

        if not primaryColumns:
            raise ConfigurationError('{} has no primary key, so an upsert cannot match its rows -- add one, or use insertStrategy: swap'.format(table))

        primaryColumnsNormalized = {column.upper() for column in primaryColumns}
        nonPrimaryColumns = [column for column in allColumns if column.upper() not in primaryColumnsNormalized]

        return allColumns, primaryColumns, nonPrimaryColumns


    def _chunkInsert(self, data: List[Tuple[Any, ...]], chunkSize: int, query: str) -> None:
        """Commits once per chunk, so a chunkSize-sized batch is the unit of work
        that survives a mid-job failure.

        range() bounds the walk at len(data) rather than testing an index inside
        the loop -- the previous `index > numberRecords` test let one extra,
        always-empty slice through whenever len(data) was an exact multiple of
        chunkSize (100 rows at chunkSize 100 issued two executemany calls, the
        second with []), which some drivers reject outright. An empty `data` is
        now an empty range, so it issues no statement at all.
        """

        for index in range(0, len(data), chunkSize):
            self.cursor.executemany(query, data[index:index + chunkSize])
            self.connection.commit()


    def insert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Optional[List[str]] = None) -> None:
        """columns defaults to introspecting the table (its full column list, in
        the table's own order); pass an explicit list to insert into a specific
        subset/order instead -- data's tuples must be in that same order.
        """

        resolvedColumns = columns if columns is not None else self.getAllColumnNames(table=table)
        columnVariables = self.dialect.placeholders(len(resolvedColumns))
        query = 'INSERT INTO {} ({}) VALUES ({})'.format(table, ', '.join(resolvedColumns), ', '.join(columnVariables))
        self._chunkInsert(data=data, chunkSize=chunkSize, query=query)


    def upsert(self, table: str, data: List[Tuple[Any, ...]], chunkSize: int = 100, columns: Optional[List[str]] = None) -> None:

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self._getColumnBuckets(table=table, columns=columns)
        query = self.dialect.upsertQuery(table=table, allColumns=allColumns, primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)
        self._chunkInsert(data=data, chunkSize=chunkSize, query=query)


    def upsertFromStage(self, targetTable: str, stageTable: str, columns: Optional[List[str]] = None) -> None:

        allColumns, primaryKeyColumns, nonPrimaryKeyColumns = self._getColumnBuckets(table=targetTable, columns=columns)
        query = self.dialect.upsertFromStageQuery(targetTable=targetTable, stageTable=stageTable, allColumns=allColumns,
                                                    primaryKeyColumns=primaryKeyColumns, nonPrimaryKeyColumns=nonPrimaryKeyColumns)
        self.alter(query=query)


    def swap(self, targetTable: str, stageTable: str) -> None:
        """Exchanges the two tables by renaming, in one transaction where the
        dialect allows it (every dialect but Oracle).

        The temporary name lives in the stage table's schema, since renaming the
        stage table is what creates it.
        """

        stageSchema, _ = splitTableName(stageTable)
        _, targetName = splitTableName(targetTable)
        tempTable = '{}.{}_tmp'.format(stageSchema, targetName) if stageSchema else targetName + '_tmp'

        for query in self.dialect.swapQueries(targetTable=targetTable, stageTable=stageTable, tempTable=tempTable):
            self.cursor.execute(query)

        self.connection.commit()
