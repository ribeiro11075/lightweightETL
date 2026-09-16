from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, List, Optional, Tuple

from .configuration import DatabaseConnectionConfig


class ColumnCategory(str, Enum):
    NUMBER = 'number'
    DATE = 'date'
    TEXT = 'text'


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
        `import lightweight_etl` doesn't require every database driver to be installed --
        only the one you actually connect with.
        """

    def streamingCursor(self, connection: Any, chunkSize: int) -> Any:
        """A cursor that does *not* buffer the whole result set client-side.

        This is what bounds an extract's memory to one chunk rather than the
        whole table, and it can only be decided per driver: a plain DB-API
        cursor.fetchmany() bounds how many rows *Python* builds objects for, but
        says nothing about how many the driver already pulled off the socket. A
        client-buffered cursor has spent the memory before fetchmany() is ever
        called.

        The default is a plain cursor, which is correct for the drivers that
        already stream row-by-row off the connection (sqlite3, pymssql).
        Dialects whose driver buffers by default override this.
        """

        return connection.cursor()


    def discardRemaining(self, connection: Any, cursor: Any) -> None:
        """Release rows left unread on `cursor`, so `connection` stays usable.

        Called when a stream is abandoned before exhaustion. The default is a
        no-op, which is correct wherever the *server* still owns the unsent rows
        and closing the cursor is enough to discard them: PostgreSQL's
        server-side cursor gets a CLOSE, and Oracle and sqlite3 drop their
        remaining rows on close. Only a driver that has already pulled rows onto
        the client connection needs to do anything here.
        """


    @abstractmethod
    def placeholders(self, count: int) -> List[str]:
        """Parameter placeholder markers, one per bound value, in this dialect's paramstyle."""

    def truncateQuery(self, table: str) -> str:
        """Standard ANSI TRUNCATE TABLE, which every dialect but SQLite supports --
        overridden there, since SQLite has no TRUNCATE statement at all.
        """

        return 'TRUNCATE TABLE {}'.format(table)

    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """Maps one raw value from cursor.description's type_code field (a shape
        that's entirely up to the driver -- a type-name string, a numeric OID, a
        driver-specific type object, ...) to a NUMBER/DATE/TEXT ColumnCategory, for
        Scramble's random-data generation (see scramble.py) to use
        without knowing or caring which database the data came from.

        None means "not recognized" -- the default here, for any dialect that
        hasn't overridden this -- and Scramble treats that exactly like an
        explicitly untyped column: shuffled rather than regenerated, never an
        error, since guessing wrong would be worse than not categorizing at all.
        """

        return None

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

    _NUMBER_TYPES = {'INT', 'BIGINT'}
    _DATE_TYPES = {'DATETIME', 'TIMESTAMP', 'DATE'}
    _TEXT_TYPES = {'TEXT', 'VARCHAR', 'CHAR'}

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import mysql.connector

        connection = mysql.connector.connect(user=settings.user, password=settings.password, host=settings.host, database=settings.database, port=settings.port)
        cursor = connection.cursor(buffered=True)

        return connection, cursor


    def streamingCursor(self, connection: Any, chunkSize: int) -> Any:
        """The inverse of connect()'s cursor: buffered=False.

        connect() deliberately uses buffered=True, which fetches the entire
        result set at execute() time -- that's what makes row counts and
        re-iteration cheap for the small metadata queries Database runs, and
        it's exactly what has to be turned off to stream a large extract.

        The trade-off an unbuffered cursor brings: it holds the connection until
        it is fully drained, so no other statement can run on this connection
        while a stream is open. _executeDataJob is safe because it reads through
        the *source* connection and writes through a separate target one --
        anything that interleaves a second query onto a streaming connection
        will raise InternalError: Unread result found.
        """

        return connection.cursor(buffered=False)


    def discardRemaining(self, connection: Any, cursor: Any) -> None:
        """mysql.connector queues unread rows on the *connection*, not the cursor.

        Closing an unbuffered cursor does not drop them, so the next statement on
        that connection fails with "InternalError: Unread result found" -- the
        connection is effectively poisoned by an abandoned stream. consume_results()
        is the driver's own remedy: it reads and discards whatever is outstanding.

        That costs a network transfer of the rows nobody wanted, which is the
        price of leaving the connection usable; it is bounded in memory, not in
        bandwidth. Abandoning a stream over a very large result set is therefore
        cheap in RAM and expensive in time on this dialect alone.
        """

        connection.consume_results()


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """mysql.connector's cursor.description reports type names as strings
        (e.g. "VARCHAR") -- a non-string dataType (shouldn't happen for this
        driver, but cheaper to guard than assume) is simply unrecognized.
        """

        if not isinstance(dataType, str):
            return None

        dataType = dataType.upper()

        if dataType in self._NUMBER_TYPES:
            return ColumnCategory.NUMBER
        if dataType in self._DATE_TYPES:
            return ColumnCategory.DATE
        if dataType in self._TEXT_TYPES:
            return ColumnCategory.TEXT

        return None


    def primaryKeyQuery(self, table: str) -> str:

        return "SELECT k.COLUMN_NAME FROM information_schema.table_constraints t LEFT JOIN information_schema.key_column_usage k USING(constraint_name, table_schema, table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_name='{}'".format(table)


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:
        """A table whose every column is part of the primary key has nothing to
        update on a conflict, and an empty SET clause is a syntax error -- so the
        conflict action becomes "do nothing" instead. OracleDialect and
        MSSQLDialect already handled this by dropping WHEN MATCHED from their
        MERGE; this is the same case on the INSERT-based dialects, which used to
        emit a dangling `ON DUPLICATE KEY UPDATE` and fail at the database.
        """

        allColumnVariables = self.placeholders(len(allColumns))

        if not nonPrimaryKeyColumns:
            return 'INSERT IGNORE INTO {} ({}) VALUES ({})'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables))

        nonPrimaryKeyColumnVariables = [column + '=VALUES(' + column + ')' for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) VALUES ({}) ON DUPLICATE KEY UPDATE {}'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables), ', '.join(nonPrimaryKeyColumnVariables))


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:
        """A table whose every column is part of the primary key has nothing to
        update on a conflict, and an empty SET clause is a syntax error -- so the
        conflict action becomes "do nothing" instead. OracleDialect and
        MSSQLDialect already handled this by dropping WHEN MATCHED from their
        MERGE; this is the same case on the INSERT-based dialects, which used to
        emit a dangling `ON DUPLICATE KEY UPDATE` and fail at the database.
        """

        if not nonPrimaryKeyColumns:
            return 'INSERT IGNORE INTO {} ({}) SELECT {} FROM {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable)

        nonPrimaryKeyColumnVariables = [column + '=VALUES(' + column + ')' for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) SELECT {} FROM {} ON DUPLICATE KEY UPDATE {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable, ', '.join(nonPrimaryKeyColumnVariables))


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:

        return ['RENAME TABLE {} TO {}, {} TO {}, {} TO {}'.format(stageTable, tempTable, targetTable, stageTable, tempTable, targetTable)]


class PostgreSQLDialect(DatabaseDialect):

    _NUMBER_OIDS = {20, 21, 23}
    _DATE_OIDS = {1114, 1018}
    _TEXT_OIDS = {1043, 18, 25}

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import psycopg2

        connection = psycopg2.connect(user=settings.user, password=settings.password, host=settings.host, database=settings.database, port=settings.port)
        cursor = connection.cursor()

        return connection, cursor


    def streamingCursor(self, connection: Any, chunkSize: int) -> Any:
        """psycopg2 only streams through a *named* cursor.

        An unnamed cursor is client-side: psycopg2 pulls the entire result set
        into the client at execute() time, so fetchmany() on one bounds nothing.
        Passing a name creates a server-side cursor (a real PostgreSQL DECLARE
        ... CURSOR), which fetches in batches of `itersize`.

        The name has to be unique within the session, hence the uuid suffix --
        two concurrent streams on one connection would otherwise collide. Note a
        server-side cursor lives inside a transaction and is invalidated by a
        commit on its connection, which is why the extract side never commits
        (Database.query/alter commit; stream() does not).
        """

        cursor = connection.cursor(name='lightweight_etl_{}'.format(uuid.uuid4().hex))
        cursor.itersize = chunkSize

        return cursor


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """psycopg2's cursor.description reports types as numeric OIDs, not names."""

        if dataType in self._NUMBER_OIDS:
            return ColumnCategory.NUMBER
        if dataType in self._DATE_OIDS:
            return ColumnCategory.DATE
        if dataType in self._TEXT_OIDS:
            return ColumnCategory.TEXT

        return None


    def primaryKeyQuery(self, table: str) -> str:

        return "SELECT c.column_name FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name=c.constraint_name WHERE t.table_name='{}' AND t.constraint_type in ('PRIMARY KEY', 'UNIQUE')".format(table)


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        """A table whose every column is part of the primary key has nothing to
        update on a conflict, and an empty SET clause is a syntax error -- so the
        conflict action becomes "do nothing" instead. OracleDialect and
        MSSQLDialect already handled this by dropping WHEN MATCHED from their
        MERGE; this is the same case on the INSERT-based dialects, which used to
        emit a dangling `DO UPDATE SET` and fail at the database.
        """

        allColumnVariables = self.placeholders(len(allColumns))

        if not nonPrimaryKeyColumns:
            return 'INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO NOTHING'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables), ','.join(primaryKeyColumns))

        nonPrimaryKeyColumnVariables = [column + '=EXCLUDED.' + column for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables), ','.join(primaryKeyColumns), ', '.join(nonPrimaryKeyColumnVariables))


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        """A table whose every column is part of the primary key has nothing to
        update on a conflict, and an empty SET clause is a syntax error -- so the
        conflict action becomes "do nothing" instead. OracleDialect and
        MSSQLDialect already handled this by dropping WHEN MATCHED from their
        MERGE; this is the same case on the INSERT-based dialects, which used to
        emit a dangling `DO UPDATE SET` and fail at the database.
        """

        if not nonPrimaryKeyColumns:
            return 'INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT({}) DO NOTHING'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable, ','.join(primaryKeyColumns))

        nonPrimaryKeyColumnVariables = [column + '=EXCLUDED.' + column for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT({}) DO UPDATE SET {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable, ','.join(primaryKeyColumns), ', '.join(nonPrimaryKeyColumnVariables))


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:

        return ['ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}'.format(stageTable, tempTable, targetTable, stageTable, tempTable, targetTable)]


class OracleDialect(DatabaseDialect):

    _NUMBER_TYPE_NAMES = {'DB_TYPE_NUMBER', 'DB_TYPE_BINARY_INTEGER', 'DB_TYPE_BINARY_FLOAT', 'DB_TYPE_BINARY_DOUBLE'}
    _DATE_TYPE_NAMES = {'DB_TYPE_DATE', 'DB_TYPE_TIMESTAMP', 'DB_TYPE_TIMESTAMP_TZ', 'DB_TYPE_TIMESTAMP_LTZ'}
    _TEXT_TYPE_NAMES = {'DB_TYPE_VARCHAR', 'DB_TYPE_CHAR', 'DB_TYPE_NVARCHAR', 'DB_TYPE_NCHAR', 'DB_TYPE_CLOB', 'DB_TYPE_NCLOB', 'DB_TYPE_LONG'}

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import oracledb

        connection = oracledb.connect(user=settings.user, password=settings.password, host=settings.host, port=settings.port,
                                       service_name=settings.serviceName, sid=settings.sid)
        cursor = connection.cursor()

        return connection, cursor


    def streamingCursor(self, connection: Any, chunkSize: int) -> Any:
        """oracledb already streams; arraysize is what makes it stream *efficiently*.

        A plain cursor fetches 100 rows per round trip by default, so a large
        extract at a large chunkSize would otherwise spend most of its time on
        network latency rather than data. prefetchrows is set one above arraysize
        -- oracledb's documented pairing, which lets the first fetch and the
        describe share a single round trip.
        """

        cursor = connection.cursor()
        cursor.arraysize = chunkSize
        cursor.prefetchrows = chunkSize + 1

        return cursor


    def placeholders(self, count: int) -> List[str]:

        return [':{}'.format(i + 1) for i in range(count)]


    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """oracledb's cursor.description reports types as oracledb.DB_TYPE_* --
        singleton objects, not strings, so this matches on their own `.name`
        attribute (e.g. "DB_TYPE_NUMBER") rather than importing oracledb just to
        compare against its constants directly -- keeps the driver import lazy and
        confined to connect(), like every other dialect here.
        """

        typeName = getattr(dataType, 'name', None)

        if typeName in self._NUMBER_TYPE_NAMES:
            return ColumnCategory.NUMBER
        if typeName in self._DATE_TYPE_NAMES:
            return ColumnCategory.DATE
        if typeName in self._TEXT_TYPE_NAMES:
            return ColumnCategory.TEXT

        return None


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

        # host is Optional[str] on DatabaseConnectionConfig only to accommodate
        # sqlite; _requireNetworkCredentialsExceptSqlite already guarantees it's
        # set for every other type, including mssql, by the time connect() runs
        assert settings.host is not None

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

    # columnCategory isn't overridden here -- pymssql's cursor.description type
    # codes are its own DBAPITypeObject constants (pymssql.NUMBER, .STRING, ...),
    # not reliably distinguishable without importing pymssql itself (unlike
    # Oracle's DB_TYPE_* objects, which expose a stable, driver-import-free `.name`
    # string). Falls back to the base class's None, so Scramble shuffles rather
    # than regenerates every MSSQL column.


class MariaDBDialect(MySQLDialect):
    """MariaDB is wire- and SQL-compatible with MySQL for everything this library
    does with it -- `INSERT ... ON DUPLICATE KEY UPDATE`, `RENAME TABLE`, and the
    same information_schema primary-key query -- so this reuses MySQLDialect's
    connect()/queries wholesale (including mysql.connector as the driver, which
    speaks MariaDB's wire protocol fine) rather than duplicating them.
    """


class SQLiteDialect(DatabaseDialect):
    """settings.database is a filesystem path (or ":memory:") -- SQLite is an
    embedded, file-based database with no server, so user/password/host/port are
    unused (DatabaseConnectionConfig only requires them for every other type).

    columnCategory isn't overridden here -- sqlite3's cursor.description always
    reports None for a column's type (SQLite is dynamically typed; there's no
    fixed type to report), so there's nothing to categorize. Falls back to the
    base class's None -- Scramble shuffles rather than regenerates every column.
    """

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import sqlite3

        # WAL, because this library reads and writes the same SQLite file from
        # two places at once. In SQLite's default rollback-journal mode a reader
        # holds a SHARED lock for as long as its statement is open, and a writer
        # on any other connection fails outright with "database is locked" --
        # which a streaming extract hits immediately whenever sourceDatabase and
        # targetDatabase are the same file, since _executeDataJob opens a
        # separate connection for each side and the read stays open across every
        # write. (This was latent before streaming too: runDataJobs runs multiple
        # worker *processes*, so two jobs writing the same file contended the
        # same way.) WAL lets one writer proceed alongside readers, which is
        # exactly that shape.
        #
        # Note this is a persistent property of the database file, not of the
        # connection -- opening a database in WAL leaves it in WAL afterwards. It
        # is a no-op for ":memory:", and requires a local filesystem: WAL uses
        # shared memory, so it does not work over NFS or SMB.
        connection = sqlite3.connect(settings.database, timeout=30.0)
        connection.execute('PRAGMA journal_mode=WAL')
        cursor = connection.cursor()

        return connection, cursor


    def placeholders(self, count: int) -> List[str]:

        return count * ['?']


    def truncateQuery(self, table: str) -> str:
        """SQLite has no TRUNCATE statement; DELETE FROM with no WHERE clears every
        row and is the documented equivalent.
        """

        return 'DELETE FROM {}'.format(table)


    def primaryKeyQuery(self, table: str) -> str:
        """pragma_table_info is SQLite's table-valued-function form of `PRAGMA
        table_info(table)` (available as a queryable "table" since 3.16, wrapping
        the equivalent PRAGMA statement) -- used instead of the bare PRAGMA so the
        result shape (one column-name column) matches every other dialect's
        primaryKeyQuery, which Database.getPrimaryColumnNames reads as row[0].
        """

        return "SELECT name FROM pragma_table_info('{}') WHERE pk > 0 ORDER BY pk".format(table)


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        """A table whose every column is part of the primary key has nothing to
        update on a conflict, and an empty SET clause is a syntax error -- so the
        conflict action becomes "do nothing" instead. OracleDialect and
        MSSQLDialect already handled this by dropping WHEN MATCHED from their
        MERGE; this is the same case on the INSERT-based dialects.
        """

        allColumnVariables = self.placeholders(len(allColumns))

        if not nonPrimaryKeyColumns:
            return 'INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO NOTHING'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables), ','.join(primaryKeyColumns))

        nonPrimaryKeyColumnVariables = [column + '=excluded.' + column for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}'.format(table, ', '.join(allColumns), ', '.join(allColumnVariables), ','.join(primaryKeyColumns), ', '.join(nonPrimaryKeyColumnVariables))


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:
        """The trailing "WHERE true" is SQLite's own documented workaround for a
        grammar ambiguity: `INSERT INTO ... SELECT ... ON CONFLICT` parses "ON" as
        if it could start a join-constraint inside the SELECT, and SQLite rejects
        the statement outright ("near \"DO\": syntax error") unless the SELECT
        carries some clause after its FROM to disambiguate -- a no-op WHERE is the
        simplest one, and doesn't affect which rows are selected.
        """

        if not nonPrimaryKeyColumns:
            return 'INSERT INTO {} ({}) SELECT {} FROM {} WHERE true ON CONFLICT({}) DO NOTHING'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable, ','.join(primaryKeyColumns))

        nonPrimaryKeyColumnVariables = [column + '=excluded.' + column for column in nonPrimaryKeyColumns]

        return 'INSERT INTO {} ({}) SELECT {} FROM {} WHERE true ON CONFLICT({}) DO UPDATE SET {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable, ','.join(primaryKeyColumns), ', '.join(nonPrimaryKeyColumnVariables))


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """Three separate statements, same reasoning as OracleDialect: sqlite3's
        cursor.execute() runs exactly one statement at a time.
        """

        return [
            'ALTER TABLE {} RENAME TO {}'.format(stageTable, tempTable),
            'ALTER TABLE {} RENAME TO {}'.format(targetTable, stageTable),
            'ALTER TABLE {} RENAME TO {}'.format(tempTable, targetTable),
            ]
