from __future__ import annotations

import re
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

from .configuration import DatabaseConnectionConfig


class ForeignKey(NamedTuple):
    """One foreign-key constraint. Composite keys list their columns in order."""

    table: str
    columns: Tuple[str, ...]
    referencedTable: str
    referencedColumns: Tuple[str, ...]
    name: str


def _groupForeignKeys(rows: Sequence[Sequence[Any]]) -> List[ForeignKey]:
    """Folds (table, column, referencedTable, referencedColumn, constraint) rows,
    already ordered by position within each constraint, into ForeignKeys.
    """

    grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for table, column, referencedTable, referencedColumn, name in rows:
        entry = grouped.setdefault((table, name), {'referencedTable': referencedTable, 'columns': [], 'referencedColumns': []})
        entry['columns'].append(column)
        entry['referencedColumns'].append(referencedColumn)

    return [
        ForeignKey(table=table, columns=tuple(entry['columns']), referencedTable=entry['referencedTable'],
                   referencedColumns=tuple(entry['referencedColumns']), name=name)
        for (table, name), entry in grouped.items()
        ]


class ColumnDefinition(NamedTuple):
    """One column as the database's catalog describes it, for generating DDL.

    `dataType` is the catalog's own type name (`character varying`, `VARCHAR2`,
    `nvarchar`, ...); schema.py maps it to a portable type. `length` is in
    characters, and is None for unbounded text or where it doesn't apply.
    """

    name: str
    dataType: str
    length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    nullable: bool


def _columnDefinitions(rows: Sequence[Sequence[Any]]) -> List[ColumnDefinition]:
    """Rows of (name, type, length, precision, scale, nullable) -> ColumnDefinitions.

    Catalogs disagree on how they say "nullable" (YES, Y, 1) and sometimes
    return numbers as Decimal, so both are normalized here.
    """

    def number(value: Any) -> Optional[int]:
        return None if value is None else int(value)

    return [
        ColumnDefinition(name=name, dataType=str(dataType), length=number(length), precision=number(precision), scale=number(scale),
                         nullable=str(nullable).upper() in ('YES', 'Y', '1', 'TRUE'))
        for name, dataType, length, precision, scale, nullable in rows
        ]


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

    def columnsQuery(self) -> str:
        """One table's columns, in order, as rows of (name, type, length,
        precision, scale, nullable). Binds the table name as its one parameter.
        """

        raise NotImplementedError('{} cannot describe columns'.format(type(self).__name__))

    def definedPrimaryKeyQuery(self) -> str:
        """One table's primary-key columns, in key order, binding the table name.

        Stricter than primaryKeyQuery, which on some dialects also returns
        UNIQUE columns: generated DDL must declare exactly the real key.
        """

        raise NotImplementedError('{} cannot describe primary keys'.format(type(self).__name__))

    def tableExistsQuery(self) -> str:
        """A count of tables with the bound name in the current schema."""

        raise NotImplementedError('{} cannot check for tables'.format(type(self).__name__))

    def columnDefinitions(self, cursor: Any, table: str) -> List[ColumnDefinition]:

        cursor.execute(self.columnsQuery().format(*self.placeholders(1)), (table,))

        return _columnDefinitions(cursor.fetchall())

    def definedPrimaryKey(self, cursor: Any, table: str) -> List[str]:

        cursor.execute(self.definedPrimaryKeyQuery().format(*self.placeholders(1)), (table,))

        return [row[0] for row in cursor.fetchall()]

    def tableExists(self, cursor: Any, table: str) -> bool:

        cursor.execute(self.tableExistsQuery().format(*self.placeholders(1)), (table,))

        return bool(cursor.fetchone()[0])

    def foreignKeysQuery(self) -> str:
        """Every foreign key in the connection's current schema, as rows of
        (table, column, referencedTable, referencedColumn, constraintName),
        ordered by table, constraint and position.
        """

        raise NotImplementedError('{} cannot list foreign keys'.format(type(self).__name__))

    def foreignKeys(self, cursor: Any) -> List[ForeignKey]:
        """Used to plan referentially complete subsets. Dialects that can't
        express this as one query (SQLite) override this instead.
        """

        cursor.execute(self.foreignKeysQuery())

        return _groupForeignKeys(cursor.fetchall())

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


    def foreignKeysQuery(self) -> str:

        return ("SELECT table_name, column_name, referenced_table_name, referenced_column_name, constraint_name "
                "FROM information_schema.key_column_usage "
                "WHERE table_schema = DATABASE() AND referenced_table_name IS NOT NULL "
                "ORDER BY table_name, constraint_name, ordinal_position")


    def columnsQuery(self) -> str:

        return ("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
                "FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = {} ORDER BY ordinal_position")


    def definedPrimaryKeyQuery(self) -> str:

        return ("SELECT column_name FROM information_schema.key_column_usage "
                "WHERE table_schema = DATABASE() AND table_name = {} AND constraint_name = 'PRIMARY' ORDER BY ordinal_position")


    def tableExistsQuery(self) -> str:

        return "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = {}"


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


    def foreignKeysQuery(self) -> str:
        """From pg_catalog rather than information_schema, which can't pair a
        composite key's columns with the columns they reference.
        """

        return ("SELECT cl.relname, att.attname, rcl.relname, ratt.attname, con.conname "
                "FROM pg_constraint con "
                "JOIN pg_class cl ON cl.oid = con.conrelid "
                "JOIN pg_namespace ns ON ns.oid = cl.relnamespace "
                "JOIN pg_class rcl ON rcl.oid = con.confrelid "
                "CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS k(attnum, refattnum, position) "
                "JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum "
                "JOIN pg_attribute ratt ON ratt.attrelid = con.confrelid AND ratt.attnum = k.refattnum "
                "WHERE con.contype = 'f' AND ns.nspname = current_schema() "
                "ORDER BY cl.relname, con.conname, k.position")


    # PostgreSQL folds unquoted names to lower case, so a table created as
    # Customers is stored as customers; the lookups below fold the same way.

    def columnsQuery(self) -> str:

        return ("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
                "FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = lower({}) ORDER BY ordinal_position")


    def definedPrimaryKeyQuery(self) -> str:

        return ("SELECT att.attname FROM pg_index idx "
                "JOIN pg_class cl ON cl.oid = idx.indrelid "
                "JOIN pg_namespace ns ON ns.oid = cl.relnamespace "
                "CROSS JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, position) "
                "JOIN pg_attribute att ON att.attrelid = cl.oid AND att.attnum = k.attnum "
                "WHERE idx.indisprimary AND ns.nspname = current_schema() AND cl.relname = lower({}) ORDER BY k.position")


    def tableExistsQuery(self) -> str:

        return "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = lower({})"


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


def _oracleLobsAsValues(cursor: Any, metadata: Any) -> Any:
    """Fetch CLOB, NCLOB and BLOB columns as str and bytes, not LOB handles.

    oracledb returns LOB locators by default, which no other driver can bind
    and which are only readable while their connection is open -- neither
    works for a value on its way to another database. Set per connection, as
    an output type handler, rather than through oracledb's process-wide
    defaults, so an application embedding this package keeps its own setting.
    """

    import oracledb

    conversions = {
        oracledb.DB_TYPE_CLOB: oracledb.DB_TYPE_LONG,
        oracledb.DB_TYPE_NCLOB: oracledb.DB_TYPE_LONG_NVARCHAR,
        oracledb.DB_TYPE_BLOB: oracledb.DB_TYPE_LONG_RAW,
        }
    conversion = conversions.get(metadata.type_code)

    return cursor.var(conversion, arraysize=cursor.arraysize) if conversion is not None else None


class OracleDialect(DatabaseDialect):

    _NUMBER_TYPE_NAMES = {'DB_TYPE_NUMBER', 'DB_TYPE_BINARY_INTEGER', 'DB_TYPE_BINARY_FLOAT', 'DB_TYPE_BINARY_DOUBLE'}
    _DATE_TYPE_NAMES = {'DB_TYPE_DATE', 'DB_TYPE_TIMESTAMP', 'DB_TYPE_TIMESTAMP_TZ', 'DB_TYPE_TIMESTAMP_LTZ'}
    _TEXT_TYPE_NAMES = {'DB_TYPE_VARCHAR', 'DB_TYPE_CHAR', 'DB_TYPE_NVARCHAR', 'DB_TYPE_NCHAR', 'DB_TYPE_CLOB', 'DB_TYPE_NCLOB', 'DB_TYPE_LONG'}

    # ISO 8601 for every implicit conversion between text and a date. Oracle's
    # default (DD-MON-RR) can't read the ISO text other databases hand over --
    # SQLite stores dates that way -- or a watermarkInitial written as
    # '1970-01-01 00:00:00'. Values that arrive as datetime objects are
    # unaffected; this only changes how text is read and written.
    SESSION_FORMATS = ("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS' "
                       "NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF' "
                       "NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import oracledb

        connection = oracledb.connect(user=settings.user, password=settings.password, host=settings.host, port=settings.port,
                                       service_name=settings.serviceName, sid=settings.sid)
        connection.outputtypehandler = _oracleLobsAsValues
        cursor = connection.cursor()
        cursor.execute(self.SESSION_FORMATS)

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


    def foreignKeysQuery(self) -> str:

        return ("SELECT c.table_name, cc.column_name, rc.table_name, rcc.column_name, c.constraint_name "
                "FROM user_constraints c "
                "JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name "
                "JOIN user_constraints rc ON rc.constraint_name = c.r_constraint_name "
                "JOIN user_cons_columns rcc ON rcc.constraint_name = rc.constraint_name AND rcc.position = cc.position "
                "WHERE c.constraint_type = 'R' "
                "ORDER BY c.table_name, c.constraint_name, cc.position")


    def columnsQuery(self) -> str:
        """CHAR_LENGTH rather than DATA_LENGTH, which is in bytes."""

        return ("SELECT column_name, data_type, CASE WHEN char_length > 0 THEN char_length END, data_precision, data_scale, nullable "
                "FROM user_tab_columns WHERE table_name = UPPER({}) ORDER BY column_id")


    def definedPrimaryKeyQuery(self) -> str:

        return ("SELECT cols.column_name FROM user_constraints cons "
                "JOIN user_cons_columns cols ON cols.constraint_name = cons.constraint_name "
                "WHERE cons.constraint_type = 'P' AND cons.table_name = UPPER({}) ORDER BY cols.position")


    def tableExistsQuery(self) -> str:

        return "SELECT count(*) FROM user_tables WHERE table_name = UPPER({})"


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


    def foreignKeysQuery(self) -> str:

        return ("SELECT tp.name, cp.name, tr.name, cr.name, fk.name "
                "FROM sys.foreign_keys fk "
                "JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id "
                "JOIN sys.tables tp ON tp.object_id = fkc.parent_object_id "
                "JOIN sys.columns cp ON cp.object_id = fkc.parent_object_id AND cp.column_id = fkc.parent_column_id "
                "JOIN sys.tables tr ON tr.object_id = fkc.referenced_object_id "
                "JOIN sys.columns cr ON cr.object_id = fkc.referenced_object_id AND cr.column_id = fkc.referenced_column_id "
                "WHERE tp.schema_id = SCHEMA_ID() "
                "ORDER BY tp.name, fk.name, fkc.constraint_column_id")


    def columnsQuery(self) -> str:
        """character_maximum_length is -1 for (MAX), which schema.py reads as unbounded."""

        return ("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
                "FROM information_schema.columns WHERE table_schema = SCHEMA_NAME() AND table_name = {} ORDER BY ordinal_position")


    def definedPrimaryKeyQuery(self) -> str:

        return ("SELECT k.column_name FROM information_schema.table_constraints t "
                "JOIN information_schema.key_column_usage k ON k.constraint_name = t.constraint_name AND k.table_schema = t.table_schema "
                "WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema = SCHEMA_NAME() AND t.table_name = {} ORDER BY k.ordinal_position")


    def tableExistsQuery(self) -> str:

        return "SELECT count(*) FROM information_schema.tables WHERE table_schema = SCHEMA_NAME() AND table_name = {}"


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


def _registerSqliteAdapters(sqlite3: Any) -> None:
    """Teach sqlite3 the value types other drivers hand back.

    sqlite3 refuses a Decimal outright -- which PostgreSQL, MySQL and SQL
    Server return for every NUMERIC column -- and its built-in date and
    timestamp adapters are deprecated since Python 3.12. Everything is stored
    as the text SQLite's own date functions read, and a Decimal as its exact
    text, which a NUMERIC column then stores as a number.

    register_adapter is process-wide, which is fine: these are the conversions
    any caller of sqlite3 would want, and registering twice is harmless.
    """

    import datetime
    import decimal

    sqlite3.register_adapter(decimal.Decimal, str)
    sqlite3.register_adapter(datetime.date, lambda value: value.isoformat())
    sqlite3.register_adapter(datetime.datetime, lambda value: value.isoformat(sep=' '))
    sqlite3.register_adapter(datetime.time, lambda value: value.isoformat())
    sqlite3.register_adapter(uuid.UUID, str)


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

        _registerSqliteAdapters(sqlite3)

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


    def columnDefinitions(self, cursor: Any, table: str) -> List[ColumnDefinition]:
        """SQLite keeps only the declared type text, e.g. `VARCHAR(50)` or
        `DECIMAL(10,2)`; the length, precision and scale are parsed out of it.
        """

        cursor.execute('SELECT name, type, "notnull", pk FROM pragma_table_info(?) ORDER BY cid', (table,))
        definitions = []

        for name, declared, notNull, primaryKey in cursor.fetchall():
            match = re.match(r'^\s*([A-Za-z ]+?)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$', declared or '')
            dataType = match.group(1) if match else (declared or '')
            first = int(match.group(2)) if match and match.group(2) else None
            second = int(match.group(3)) if match and match.group(3) else None
            numeric = any(word in dataType.upper() for word in ('DEC', 'NUM'))
            definitions.append(ColumnDefinition(
                name=name, dataType=dataType, length=None if numeric else first, precision=first if numeric else None,
                scale=second if numeric else None, nullable=not notNull and not primaryKey))

        return definitions


    def definedPrimaryKey(self, cursor: Any, table: str) -> List[str]:

        cursor.execute('SELECT name FROM pragma_table_info(?) WHERE pk > 0 ORDER BY pk', (table,))

        return [row[0] for row in cursor.fetchall()]


    def tableExists(self, cursor: Any, table: str) -> bool:

        cursor.execute("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND lower(name) = lower(?)", (table,))

        return bool(cursor.fetchone()[0])


    def foreignKeys(self, cursor: Any) -> List[ForeignKey]:
        """SQLite keeps foreign keys per table, behind a pragma, so this lists
        the tables and asks each. A reference that omits its columns means the
        referenced table's primary key, which is resolved here.
        """

        cursor.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        tables = [row[0] for row in cursor.fetchall()]
        rows = []

        for table in tables:
            cursor.execute('SELECT id, "table", "from", "to" FROM pragma_foreign_key_list(?) ORDER BY id, seq', (table,))
            references = cursor.fetchall()

            primaryKeys: Dict[str, List[str]] = {}
            positions: Dict[int, int] = {}

            for constraintId, referencedTable, column, referencedColumn in references:
                position = positions.get(constraintId, 0)
                positions[constraintId] = position + 1

                if referencedColumn is None:
                    if referencedTable not in primaryKeys:
                        cursor.execute(self.primaryKeyQuery(referencedTable))
                        primaryKeys[referencedTable] = [row[0] for row in cursor.fetchall()]
                    referencedColumn = primaryKeys[referencedTable][position]

                rows.append((table, column, referencedTable, referencedColumn, '{}_fk{}'.format(table, constraintId)))

        return _groupForeignKeys(rows)


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
