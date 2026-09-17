from __future__ import annotations

import datetime
import decimal
import hashlib
import io
import math
import re
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

from .configuration import ConfigurationError, DatabaseConnectionConfig


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


def splitTableName(table: str) -> Tuple[Optional[str], str]:
    """`schema.table` -> ('schema', 'table'); a bare `table` -> (None, 'table').

    None means the connection's current schema, which is what every catalog
    lookup below falls back to. Looking a table up without a schema at all is
    how a same-named table in another schema used to leak its key columns into
    an upsert.
    """

    schema, _, name = table.rpartition('.')

    return schema or None, name


def unqualifiedName(table: str) -> str:
    """The name without its schema -- what `RENAME TO` and sp_rename take."""

    return splitTableName(table)[1]


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
        `import understudy_data` doesn't require every database driver to be installed --
        only the one you actually connect with.
        """

    @abstractmethod
    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:
        """The driver keyword arguments the connection fields map to."""

    def connectArguments(self, settings: DatabaseConnectionConfig, resolvePassword: bool = True) -> Dict[str, Any]:
        """Everything passed to the driver's connect(): the fields' own
        arguments, plus settings.options.

        An option that names an argument a field already sets is refused rather
        than silently winning or losing -- set the field instead. Needs no
        connection, so `validate` checks it offline -- with resolvePassword
        False, so that checking never runs a passwordCommand.
        """

        own = self._ownConnectArguments(settings, settings.plainPassword() if resolvePassword else None)
        clashes = sorted(set(own) & set(settings.options))

        if clashes:
            raise ConfigurationError('options {} duplicate what the connection fields already set for {}; use the fields instead'.format(
                ', '.join(clashes), settings.type.value))

        return {**own, **settings.options}

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

    def supportsMaterializedSelections(self) -> bool:
        """Whether `WITH name AS MATERIALIZED (...)` is accepted, and worth
        using: planSubset's queries then compute each selection once.
        """

        return False

    def isEncrypted(self, cursor: Any) -> Optional[bool]:
        """Whether this connection is encrypted in transit, as the server
        reports it -- which is what an auditor wants, rather than what the
        connection settings asked for. None where there is no network (SQLite)
        or no way to tell.
        """

        return None

    def bulkInsert(self, cursor: Any, table: str, columns: List[str], rows: Sequence[Sequence[Any]]) -> bool:
        """Loads `rows` in fewer round trips than one statement per row, where
        the driver doesn't already do that for executemany (psycopg2 and
        pymssql don't). False means nothing was sent, and the caller should
        insert them statement by statement instead.
        """

        return False

    def bulkUpsert(self, cursor: Any, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str],
                   rows: Sequence[Sequence[Any]]) -> bool:
        """bulkInsert for an upsert: the same contract. `rows` hold no two rows
        with the same key.
        """

        return False

    def truncateQuery(self, table: str) -> str:
        """Standard ANSI TRUNCATE TABLE, which every dialect but SQLite supports --
        overridden there, since SQLite has no TRUNCATE statement at all.
        """

        return 'TRUNCATE TABLE {}'.format(table)

    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """Maps one raw value from cursor.description's type_code field (a shape
        that's entirely up to the driver -- a type-name string, a numeric OID, a
        driver-specific type object, ...) to a NUMBER/DATE/TEXT ColumnCategory, for
        discovery.py to classify columns without knowing or caring which
        database the data came from.

        None means "not recognized" -- the default here, for any dialect that
        hasn't overridden this -- and discovery then infers a category from
        sampled values instead.
        """

        return None

    # The three catalog queries below each bind two parameters, the schema and
    # the table, from splitTableName. A NULL schema means the current one.

    def primaryKeyQuery(self) -> str:
        """One table's primary-key columns, in key order.

        The declared primary key only. UNIQUE constraints are left out: an
        upsert matching on (id, email) treats a row whose email changed as a new
        row, and PostgreSQL rejects an ON CONFLICT list no single index matches.
        """

        raise NotImplementedError('{} cannot describe primary keys'.format(type(self).__name__))

    def columnsQuery(self) -> str:
        """One table's columns, in order, as rows of (name, type, length,
        precision, scale, nullable).
        """

        raise NotImplementedError('{} cannot describe columns'.format(type(self).__name__))

    def tableExistsQuery(self) -> str:
        """A count of tables with the bound name in the bound schema."""

        raise NotImplementedError('{} cannot check for tables'.format(type(self).__name__))

    def _catalog(self, cursor: Any, query: str, table: str) -> List[Any]:

        cursor.execute(query.format(*self.placeholders(2)), splitTableName(table))

        return cursor.fetchall()

    def primaryKey(self, cursor: Any, table: str) -> List[str]:

        return [row[0] for row in self._catalog(cursor, self.primaryKeyQuery(), table)]

    def columnDefinitions(self, cursor: Any, table: str) -> List[ColumnDefinition]:

        return _columnDefinitions(self._catalog(cursor, self.columnsQuery(), table))

    def tableExists(self, cursor: Any, table: str) -> bool:

        return bool(self._catalog(cursor, self.tableExistsQuery(), table)[0][0])

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
        """One or more statements to execute in order, then commit once.

        `tempTable` is in the stage table's schema, and `stageTable` must share
        the target's (configuration checks that), since a rename never moves a
        table between schemas. Renames take the new name unqualified.
        """

    def swap(self, cursor: Any, targetTable: str, stageTable: str, tempTable: str) -> None:
        """Runs the swap on `cursor`; the caller commits. A dialect with more to
        do around the renames overrides this.
        """

        for query in self.swapQueries(targetTable=targetTable, stageTable=stageTable, tempTable=tempTable):
            cursor.execute(query)


class _OnConflictDialect(DatabaseDialect):
    """PostgreSQL and SQLite share `INSERT ... ON CONFLICT` word for word.

    A table whose every column is part of the primary key has nothing to update
    on a conflict, and an empty SET clause is a syntax error, so the conflict
    action becomes DO NOTHING. The MERGE dialects drop WHEN MATCHED instead.

    The stage form's `WHERE true` is SQLite's documented workaround for a
    grammar ambiguity: without a clause after FROM, it reads ON as the start of
    a join constraint and rejects the statement. PostgreSQL accepts it as the
    no-op it is.
    """

    @staticmethod
    def _onConflict(primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        if not nonPrimaryKeyColumns:
            return 'ON CONFLICT({}) DO NOTHING'.format(','.join(primaryKeyColumns))

        return 'ON CONFLICT({}) DO UPDATE SET {}'.format(
            ','.join(primaryKeyColumns), ', '.join('{0}=excluded.{0}'.format(column) for column in nonPrimaryKeyColumns))

    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        return 'INSERT INTO {} ({}) VALUES ({}) {}'.format(table, ', '.join(allColumns), ', '.join(self.placeholders(len(allColumns))),
                                                          self._onConflict(primaryKeyColumns, nonPrimaryKeyColumns))

    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        return 'INSERT INTO {} ({}) SELECT {} FROM {} WHERE true {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable,
                                                                          self._onConflict(primaryKeyColumns, nonPrimaryKeyColumns))


class MySQLDialect(DatabaseDialect):

    _NUMBER_TYPES = {'INT', 'BIGINT'}
    _DATE_TYPES = {'DATETIME', 'TIMESTAMP', 'DATE'}
    _TEXT_TYPES = {'TEXT', 'VARCHAR', 'CHAR'}

    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:

        return {'user': settings.user, 'password': password, 'host': settings.host, 'database': settings.database,
                'port': settings.port}


    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import mysql.connector

        connection = mysql.connector.connect(**self.connectArguments(settings))
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


    def isEncrypted(self, cursor: Any) -> Optional[bool]:

        cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        row = cursor.fetchone()

        return None if row is None else bool(row[1])


    def foreignKeysQuery(self) -> str:

        return ("SELECT table_name, column_name, referenced_table_name, referenced_column_name, constraint_name "
                "FROM information_schema.key_column_usage "
                "WHERE table_schema = DATABASE() AND referenced_table_name IS NOT NULL "
                "ORDER BY table_name, constraint_name, ordinal_position")


    def columnsQuery(self) -> str:

        return ("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
                "FROM information_schema.columns WHERE table_schema = COALESCE({}, DATABASE()) AND table_name = {} ORDER BY ordinal_position")


    def primaryKeyQuery(self) -> str:

        return ("SELECT column_name FROM information_schema.key_column_usage "
                "WHERE table_schema = COALESCE({}, DATABASE()) AND table_name = {} AND constraint_name = 'PRIMARY' ORDER BY ordinal_position")


    def tableExistsQuery(self) -> str:

        return "SELECT count(*) FROM information_schema.tables WHERE table_schema = COALESCE({}, DATABASE()) AND table_name = {}"


    @staticmethod
    def _onDuplicateKey(table: str, primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:
        """A key-only table has nothing to update, so the update is a no-op
        assignment of its own key -- an empty SET clause is a syntax error.

        Not INSERT IGNORE, which also downgrades truncation, NOT NULL and
        foreign-key errors to warnings, silently dropping or mangling rows.
        """

        if not nonPrimaryKeyColumns:
            return 'ON DUPLICATE KEY UPDATE {0}.{1}={0}.{1}'.format(table, primaryKeyColumns[0])

        return 'ON DUPLICATE KEY UPDATE {}'.format(', '.join('{0}=VALUES({0})'.format(column) for column in nonPrimaryKeyColumns))


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        return 'INSERT INTO {} ({}) VALUES ({}) {}'.format(table, ', '.join(allColumns), ', '.join(self.placeholders(len(allColumns))),
                                                          self._onDuplicateKey(table, primaryKeyColumns, nonPrimaryKeyColumns))


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        return 'INSERT INTO {} ({}) SELECT {} FROM {} {}'.format(targetTable, ', '.join(allColumns), ', '.join(allColumns), stageTable,
                                                                self._onDuplicateKey(targetTable, primaryKeyColumns, nonPrimaryKeyColumns))


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """One atomic statement; RENAME TABLE takes qualified names on both sides."""

        return ['RENAME TABLE {} TO {}, {} TO {}, {} TO {}'.format(stageTable, tempTable, targetTable, stageTable, tempTable, targetTable)]


class _Unencodable(Exception):
    """A value COPY's text format has no safe spelling for, here."""


_COPY_ESCAPES = str.maketrans({'\\': '\\\\', '\t': '\\t', '\n': '\\n', '\r': '\\r'})


def _copyField(value: Any) -> str:
    """One value in PostgreSQL's COPY text format.

    Only types whose text form PostgreSQL parses back exactly are handled;
    anything else -- a list, a dict, a timedelta -- raises _Unencodable, and
    the chunk goes through the driver's own adapters instead.
    """

    if value is None:
        return '\\N'
    if isinstance(value, bool):
        return 't' if value else 'f'
    if isinstance(value, (int, decimal.Decimal)):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return 'NaN'
        if math.isinf(value):
            return 'Infinity' if value > 0 else '-Infinity'
        return repr(value)
    if isinstance(value, str):
        return value.translate(_COPY_ESCAPES)
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=' ')
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        # bytea's hex input, with its backslash escaped for the text format.
        return '\\\\x' + bytes(value).hex()

    raise _Unencodable(type(value).__name__)


def _copyText(rows: Sequence[Sequence[Any]]) -> Optional[io.StringIO]:
    """The rows as a COPY text-format stream, or None if any value can't be encoded."""

    try:
        text = ''.join('\t'.join(_copyField(value) for value in row) + '\n' for row in rows)
    except _Unencodable:
        return None

    return io.StringIO(text)


class PostgreSQLDialect(_OnConflictDialect):

    _NUMBER_OIDS = {20, 21, 23}
    _DATE_OIDS = {1114, 1018}
    _TEXT_OIDS = {1043, 18, 25}

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import psycopg2

        connection = psycopg2.connect(**self.connectArguments(settings))
        cursor = connection.cursor()

        # Only the one schema: a fallback such as `public` would send an
        # unqualified write to a table there while every catalog lookup
        # (current_schema()) looked here. Committed, since a SET inside a
        # transaction that is later rolled back is undone with it.
        if settings.currentSchema:
            cursor.execute('SET search_path TO {}'.format(settings.currentSchema))
            connection.commit()

        return connection, cursor


    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:

        return {'user': settings.user, 'password': password, 'host': settings.host, 'database': settings.database,
                'port': settings.port}


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

        cursor = connection.cursor(name='understudy_{}'.format(uuid.uuid4().hex))
        cursor.itersize = chunkSize

        return cursor


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


    def supportsMaterializedSelections(self) -> bool:
        """PostgreSQL 12 and later. Without it, PostgreSQL copies a selection
        used once into its user, and planning a 12-table subset took minutes.
        """

        return True


    def isEncrypted(self, cursor: Any) -> Optional[bool]:

        cursor.execute('SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()')
        row = cursor.fetchone()

        return None if row is None else bool(row[0])


    def bulkInsert(self, cursor: Any, table: str, columns: List[str], rows: Sequence[Sequence[Any]]) -> bool:
        """COPY FROM STDIN: one round trip per chunk, where psycopg2's
        executemany sends one statement per row.
        """

        stream = _copyText(rows)
        if stream is None:
            return False

        cursor.copy_expert('COPY {} ({}) FROM STDIN'.format(table, ', '.join(columns)), stream)

        return True


    def bulkUpsert(self, cursor: Any, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str],
                   rows: Sequence[Sequence[Any]]) -> bool:
        """COPY into a temporary table shaped like the target's columns, then
        one INSERT ... ON CONFLICT from it.

        The temporary table takes only the columns' types -- no constraints,
        defaults or identity -- and empties itself at every commit, so it is
        created once per connection and column list, and reused by each chunk.
        """

        stream = _copyText(rows)
        if stream is None:
            return False

        columns = ', '.join(allColumns)
        staging = 'understudy_upsert_{}'.format(hashlib.sha1('{}|{}'.format(table, columns).encode('utf-8')).hexdigest()[:12])

        cursor.execute('CREATE TEMPORARY TABLE IF NOT EXISTS {} ON COMMIT DELETE ROWS AS SELECT {} FROM {} WITH NO DATA'.format(staging, columns, table))
        cursor.copy_expert('COPY {} ({}) FROM STDIN'.format(staging, columns), stream)
        cursor.execute(self.upsertFromStageQuery(table, staging, allColumns, primaryKeyColumns, nonPrimaryKeyColumns))

        return True


    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """psycopg2's cursor.description reports types as numeric OIDs, not names."""

        if dataType in self._NUMBER_OIDS:
            return ColumnCategory.NUMBER
        if dataType in self._DATE_OIDS:
            return ColumnCategory.DATE
        if dataType in self._TEXT_OIDS:
            return ColumnCategory.TEXT

        return None


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
    # The ::text casts give a NULL schema a type, which lower() needs.

    def columnsQuery(self) -> str:

        return ("SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable "
                "FROM information_schema.columns WHERE table_schema = COALESCE(lower({}::text), current_schema()) "
                "AND table_name = lower({}::text) ORDER BY ordinal_position")


    def primaryKeyQuery(self) -> str:

        return ("SELECT att.attname FROM pg_index idx "
                "JOIN pg_class cl ON cl.oid = idx.indrelid "
                "JOIN pg_namespace ns ON ns.oid = cl.relnamespace "
                "CROSS JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, position) "
                "JOIN pg_attribute att ON att.attrelid = cl.oid AND att.attnum = k.attnum "
                "WHERE idx.indisprimary AND ns.nspname = COALESCE(lower({}::text), current_schema()) AND cl.relname = lower({}::text) "
                "ORDER BY k.position")


    def tableExistsQuery(self) -> str:

        return ("SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = COALESCE(lower({}::text), current_schema()) AND table_name = lower({}::text)")


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """Three renames in one transaction: PostgreSQL DDL is transactional, so
        a failure part-way leaves both tables as they were.
        """

        return ['ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}; ALTER TABLE {} RENAME TO {}'.format(
            stageTable, unqualifiedName(tempTable), targetTable, unqualifiedName(stageTable), tempTable, unqualifiedName(targetTable))]


    # Views built directly on a table: their names, and their definitions as
    # PostgreSQL would write them now, table names and all.
    DEPENDENT_VIEWS_QUERY = (
        "SELECT DISTINCT view.oid::regclass::text, pg_get_viewdef(view.oid) "
        "FROM pg_depend dependency "
        "JOIN pg_rewrite rewrite ON rewrite.oid = dependency.objid "
        "JOIN pg_class view ON view.oid = rewrite.ev_class "
        "WHERE dependency.classid = 'pg_rewrite'::regclass AND dependency.refobjid = %s::regclass "
        "AND view.oid <> dependency.refobjid AND view.relkind = 'v'")

    def swap(self, cursor: Any, targetTable: str, stageTable: str, tempTable: str) -> None:
        """Renames, then points the target's views at the new target.

        A PostgreSQL view is bound to the table it was created on, not to that
        table's name, so after the renames it would read what is now the stage
        table -- the old data, emptied by the next run. Each view built directly
        on the target is recreated from its own definition, captured before the
        renames, whose table name now resolves to the new target. CREATE OR
        REPLACE keeps the view itself, so its grants and any views built on it
        stay as they were. It all happens in the swap's transaction.

        Materialized views and foreign keys referencing the target aren't
        rebound; see docs/design.md.
        """

        cursor.execute(self.DEPENDENT_VIEWS_QUERY, (targetTable,))
        views = cursor.fetchall()

        super().swap(cursor, targetTable, stageTable, tempTable)

        for name, definition in views:
            cursor.execute('CREATE OR REPLACE VIEW {} AS {}'.format(name, definition))


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


def _renameInThreeSteps(targetTable: str, stageTable: str, tempTable: str) -> List[str]:

    return [
        'ALTER TABLE {} RENAME TO {}'.format(stageTable, unqualifiedName(tempTable)),
        'ALTER TABLE {} RENAME TO {}'.format(targetTable, unqualifiedName(stageTable)),
        'ALTER TABLE {} RENAME TO {}'.format(tempTable, unqualifiedName(targetTable)),
        ]


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

        connection = oracledb.connect(**self.connectArguments(settings))
        connection.outputtypehandler = _oracleLobsAsValues
        cursor = connection.cursor()
        cursor.execute(self.SESSION_FORMATS)

        if settings.currentSchema:
            cursor.execute('ALTER SESSION SET CURRENT_SCHEMA = {}'.format(settings.currentSchema))

        return connection, cursor


    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:

        return {'user': settings.user, 'password': password, 'host': settings.host, 'port': settings.port,
                'service_name': settings.serviceName, 'sid': settings.sid}


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


    def foreignKeysQuery(self) -> str:

        return ("SELECT c.table_name, cc.column_name, rc.table_name, rcc.column_name, c.constraint_name "
                "FROM user_constraints c "
                "JOIN user_cons_columns cc ON cc.constraint_name = c.constraint_name "
                "JOIN user_constraints rc ON rc.constraint_name = c.r_constraint_name "
                "JOIN user_cons_columns rcc ON rcc.constraint_name = rc.constraint_name AND rcc.position = cc.position "
                "WHERE c.constraint_type = 'R' "
                "ORDER BY c.table_name, c.constraint_name, cc.position")


    # The all_* views, filtered to one owner: the bound schema, or else the
    # session's current schema -- which ALTER SESSION SET CURRENT_SCHEMA moves
    # and the user_* views would not follow.
    OWNER = "COALESCE(UPPER({}), SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))"

    def columnsQuery(self) -> str:
        """CHAR_LENGTH rather than DATA_LENGTH, which is in bytes."""

        return ("SELECT column_name, data_type, CASE WHEN char_length > 0 THEN char_length END, data_precision, data_scale, nullable "
                "FROM all_tab_columns WHERE owner = " + self.OWNER + " AND table_name = UPPER({}) ORDER BY column_id")


    def isEncrypted(self, cursor: Any) -> Optional[bool]:

        cursor.execute("SELECT SYS_CONTEXT('USERENV', 'NETWORK_PROTOCOL') FROM dual")
        protocol = cursor.fetchone()[0]

        return None if protocol is None else protocol.lower() == 'tcps'


    def primaryKeyQuery(self) -> str:

        return ("SELECT cols.column_name FROM all_constraints cons "
                "JOIN all_cons_columns cols ON cols.owner = cons.owner AND cols.constraint_name = cons.constraint_name "
                "WHERE cons.constraint_type = 'P' AND cons.owner = " + self.OWNER + " AND cons.table_name = UPPER({}) ORDER BY cols.position")


    def tableExistsQuery(self) -> str:

        return "SELECT count(*) FROM all_tables WHERE owner = " + self.OWNER + " AND table_name = UPPER({})"


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        bindColumns = ', '.join('{} {}'.format(placeholder, column) for placeholder, column in zip(self.placeholders(len(allColumns)), allColumns))
        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        return 'MERGE INTO {} target USING (SELECT {} FROM dual) source {}'.format(table, bindColumns, mergeClause)


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        return 'MERGE INTO {} target USING {} source {}'.format(targetTable, stageTable, mergeClause)


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """Three separate statements: Oracle's cursor.execute() runs exactly one.

        Oracle commits every DDL statement on its own, so unlike the other
        dialects this swap is not atomic. A failure between the renames leaves
        the target under the temporary name, and the job's error says so.
        """

        return _renameInThreeSteps(targetTable, stageTable, tempTable)


class MSSQLDialect(DatabaseDialect):

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import pymssql

        connection = pymssql.connect(**self.connectArguments(settings))
        cursor = connection.cursor()

        return connection, cursor


    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:
        """pymssql's port is a str, and unlike the other drivers it doesn't fall
        back to its own default ('1433') when passed None, so it's left out
        entirely when unset.
        """

        arguments: Dict[str, Any] = {'server': settings.host, 'user': settings.user, 'password': password, 'database': settings.database}
        if settings.port is not None:
            arguments['port'] = str(settings.port)

        return arguments


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


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
                "FROM information_schema.columns WHERE table_schema = COALESCE({}, SCHEMA_NAME()) AND table_name = {} ORDER BY ordinal_position")


    def isEncrypted(self, cursor: Any) -> Optional[bool]:
        """Needs VIEW SERVER STATE; without it the query fails, and the answer
        is None.
        """

        cursor.execute('SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID')
        row = cursor.fetchone()

        return None if row is None else str(row[0]).upper() == 'TRUE'


    def primaryKeyQuery(self) -> str:

        return ("SELECT k.column_name FROM information_schema.table_constraints t "
                "JOIN information_schema.key_column_usage k ON k.constraint_name = t.constraint_name AND k.table_schema = t.table_schema "
                "WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema = COALESCE({}, SCHEMA_NAME()) AND t.table_name = {} "
                "ORDER BY k.ordinal_position")


    def tableExistsQuery(self) -> str:

        return "SELECT count(*) FROM information_schema.tables WHERE table_schema = COALESCE({}, SCHEMA_NAME()) AND table_name = {}"


    def upsertQuery(self, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str], rowCount: int = 1) -> str:

        rowValues = ', '.join(['({})'.format(', '.join(self.placeholders(len(allColumns))))] * rowCount)
        columnNames = ', '.join(allColumns)
        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        # MERGE requires a terminating semicolon in T-SQL, unlike Oracle
        return 'MERGE INTO {} AS target USING (VALUES {}) AS source ({}) {};'.format(table, rowValues, columnNames, mergeClause)


    # A VALUES list in an INSERT takes at most 1000 rows. pymssql binds
    # parameters by quoting them into the statement itself, so SQL Server's
    # 2100-parameter limit doesn't apply.
    VALUES_ROW_LIMIT = 1000

    def bulkInsert(self, cursor: Any, table: str, columns: List[str], rows: Sequence[Sequence[Any]]) -> bool:
        """Multi-row INSERT ... VALUES: pymssql's executemany sends a statement per row."""

        rowValues = '({})'.format(', '.join(self.placeholders(len(columns))))

        for offset in range(0, len(rows), self.VALUES_ROW_LIMIT):
            batch = rows[offset:offset + self.VALUES_ROW_LIMIT]
            cursor.execute('INSERT INTO {} ({}) VALUES {}'.format(table, ', '.join(columns), ', '.join([rowValues] * len(batch))),
                           tuple(value for row in batch for value in row))

        return True


    def bulkUpsert(self, cursor: Any, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str],
                   rows: Sequence[Sequence[Any]]) -> bool:
        """One MERGE per thousand rows. `rows` hold one row per key, which MERGE
        requires: it refuses to update a target row twice.
        """

        for offset in range(0, len(rows), self.VALUES_ROW_LIMIT):
            batch = rows[offset:offset + self.VALUES_ROW_LIMIT]
            cursor.execute(self.upsertQuery(table, allColumns, primaryKeyColumns, nonPrimaryKeyColumns, rowCount=len(batch)),
                           tuple(value for row in batch for value in row))

        return True


    def upsertFromStageQuery(self, targetTable: str, stageTable: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str]) -> str:

        mergeClause = _mergeUpdateInsertClause('target', 'source', allColumns, primaryKeyColumns, nonPrimaryKeyColumns)

        return 'MERGE INTO {} AS target USING {} AS source {};'.format(targetTable, stageTable, mergeClause)


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """sp_rename is a stored procedure, not DDL -- EXEC calls chain fine in one
        execute(), so (unlike Oracle) this doesn't need three separate statements.
        It runs inside the connection's transaction, so the swap is atomic.

        The new name is taken literally: a qualified one would create a table
        whose name contains the dot.
        """

        return ["EXEC sp_rename '{}', '{}'; EXEC sp_rename '{}', '{}'; EXEC sp_rename '{}', '{}';".format(
            stageTable, unqualifiedName(tempTable), targetTable, unqualifiedName(stageTable), tempTable, unqualifiedName(targetTable))]

    # columnCategory isn't overridden here -- pymssql's cursor.description type
    # codes are its own DBAPITypeObject constants (pymssql.NUMBER, .STRING, ...),
    # not reliably distinguishable without importing pymssql itself (unlike
    # Oracle's DB_TYPE_* objects, which expose a stable, driver-import-free `.name`
    # string). Falls back to the base class's None, so discovery infers each
    # column's category from sampled values instead.


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


class SQLiteDialect(_OnConflictDialect):
    """settings.database is a filesystem path (or ":memory:") -- SQLite is an
    embedded, file-based database with no server, so user/password/host/port are
    unused (DatabaseConnectionConfig only requires them for every other type).

    columnCategory isn't overridden here -- sqlite3's cursor.description always
    reports None for a column's type (SQLite is dynamically typed; there's no
    fixed type to report), so there's nothing to categorize. Falls back to the
    base class's None, and discovery infers a category from sampled values.
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
        connection = sqlite3.connect(**self.connectArguments(settings))
        connection.execute('PRAGMA journal_mode=WAL')
        cursor = connection.cursor()

        return connection, cursor


    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:

        return {'database': settings.database, 'timeout': 30.0}


    def placeholders(self, count: int) -> List[str]:

        return count * ['?']


    def supportsMaterializedSelections(self) -> bool:
        """SQLite 3.35 and later, which is what Python's own sqlite3 links
        against on most platforms -- but not all, hence the check.
        """

        import sqlite3

        return sqlite3.sqlite_version_info >= (3, 35)


    def truncateQuery(self, table: str) -> str:
        """SQLite has no TRUNCATE statement; DELETE FROM with no WHERE clears every
        row and is the documented equivalent.
        """

        return 'DELETE FROM {}'.format(table)


    # SQLite describes tables through pragma table-valued functions, whose
    # optional second argument is the attached database -- SQLite's schema.

    def primaryKey(self, cursor: Any, table: str) -> List[str]:

        schema, name = splitTableName(table)
        cursor.execute('SELECT name FROM pragma_table_info(?, ?) WHERE pk > 0 ORDER BY pk', (name, schema or 'main'))

        return [row[0] for row in cursor.fetchall()]


    def columnDefinitions(self, cursor: Any, table: str) -> List[ColumnDefinition]:
        """SQLite keeps only the declared type text, e.g. `VARCHAR(50)` or
        `DECIMAL(10,2)`; the length, precision and scale are parsed out of it.
        """

        schema, name = splitTableName(table)
        cursor.execute('SELECT name, type, "notnull", pk FROM pragma_table_info(?, ?) ORDER BY cid', (name, schema or 'main'))
        definitions = []

        for columnName, declared, notNull, primaryKey in cursor.fetchall():
            match = re.match(r'^\s*([A-Za-z ]+?)\s*(?:\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\))?\s*$', declared or '')
            dataType = match.group(1) if match else (declared or '')
            first = int(match.group(2)) if match and match.group(2) else None
            second = int(match.group(3)) if match and match.group(3) else None
            numeric = any(word in dataType.upper() for word in ('DEC', 'NUM'))
            definitions.append(ColumnDefinition(
                name=columnName, dataType=dataType, length=None if numeric else first, precision=first if numeric else None,
                scale=second if numeric else None, nullable=not notNull and not primaryKey))

        return definitions


    def tableExists(self, cursor: Any, table: str) -> bool:

        schema, name = splitTableName(table)
        cursor.execute("SELECT count(*) FROM {}.sqlite_master WHERE type = 'table' AND lower(name) = lower(?)".format(schema or 'main'), (name,))

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
                        primaryKeys[referencedTable] = self.primaryKey(cursor, referencedTable)
                    referencedColumn = primaryKeys[referencedTable][position]

                rows.append((table, column, referencedTable, referencedColumn, '{}_fk{}'.format(table, constraintId)))

        return _groupForeignKeys(rows)


    def swapQueries(self, targetTable: str, stageTable: str, tempTable: str) -> List[str]:
        """Three statements, since sqlite3's cursor.execute() runs one at a time,
        inside an explicit transaction. sqlite3 doesn't open one implicitly
        before DDL, so without the BEGIN each rename would commit on its own and
        a failure part-way would leave the target missing.
        """

        return ['BEGIN'] + _renameInThreeSteps(targetTable, stageTable, tempTable)
