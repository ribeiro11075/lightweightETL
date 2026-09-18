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

from .configuration import IDENTIFIER, ConfigurationError, DatabaseConnectionConfig, DatabaseType


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
    """`schema.table` -> ('schema', 'table'); a bare `table` -> (None, 'table'),
    None meaning the connection's current schema.
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
    """The ON/WHEN MATCHED/WHEN NOT MATCHED tail of an Oracle or SQL Server
    MERGE. A key-only table gets no WHEN MATCHED, since an empty SET is invalid.
    """

    onClause = ' AND '.join('{}.{} = {}.{}'.format(targetAlias, column, sourceAlias, column) for column in primaryKeyColumns)
    insertColumns = ', '.join(allColumns)
    insertValues = ', '.join('{}.{}'.format(sourceAlias, column) for column in allColumns)

    whenMatched = ''
    if nonPrimaryKeyColumns:
        updateClause = ', '.join('{}.{} = {}.{}'.format(targetAlias, column, sourceAlias, column) for column in nonPrimaryKeyColumns)
        whenMatched = 'WHEN MATCHED THEN UPDATE SET {} '.format(updateClause)

    return 'ON ({}) {}WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})'.format(onClause, whenMatched, insertColumns, insertValues)


# How each database quotes an identifier, where it isn't with double quotes.
_IDENTIFIER_QUOTES = {DatabaseType.MYSQL: ('`', '`'), DatabaseType.MARIADB: ('`', '`'), DatabaseType.MSSQL: ('[', ']')}

# How a database folds an unquoted identifier, where it doesn't keep it as
# written. The others compare identifiers case-insensitively anyway.
_UNQUOTED_CASE = {DatabaseType.ORACLE: str.upper, DatabaseType.POSTGRESQL: str.lower}


def quoteIdentifier(databaseType: DatabaseType, name: str) -> str:
    """`name`, quoted, so a reserved word (`rank`, `order`) works as a column
    name. Quoting makes the name case-sensitive on Oracle and PostgreSQL, so
    `name` must be spelled as the catalog spells it.
    """

    opening, closing = _IDENTIFIER_QUOTES.get(databaseType, ('"', '"'))

    return opening + name.replace(closing, closing * 2) + closing


def quoteFolded(databaseType: DatabaseType, name: str) -> str:
    """`name` quoted as the database would store it unquoted -- upper case on
    Oracle, lower case on PostgreSQL -- for a column being created, which
    then answers to the same unquoted name it would have without quotes.
    """

    fold = _UNQUOTED_CASE.get(databaseType)
    if fold is not None and IDENTIFIER.match(name):
        name = fold(name)

    return quoteIdentifier(databaseType, name)


class DatabaseDialect(ABC):
    """Everything that differs between database types lives here, not in Database."""

    @abstractmethod
    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:
        """Returns (connection, cursor). Drivers are imported here, so only the
        one in use needs installing.
        """

    @abstractmethod
    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:
        """The driver keyword arguments the connection fields map to."""

    def connectArguments(self, settings: DatabaseConnectionConfig, resolvePassword: bool = True) -> Dict[str, Any]:
        """The fields' driver arguments plus settings.options, refusing an
        option that duplicates a field. resolvePassword=False lets `validate`
        check this without running a passwordCommand.
        """

        own = self._ownConnectArguments(settings, settings.plainPassword() if resolvePassword else None)
        clashes = sorted(set(own) & set(settings.options))

        if clashes:
            raise ConfigurationError('options {} duplicate what the connection fields already set for {}; use the fields instead'.format(
                ', '.join(clashes), settings.type.value))

        return {**own, **settings.options}

    def streamingCursor(self, connection: Any, chunkSize: int) -> Any:
        """A cursor that doesn't buffer the whole result set client-side, which
        fetchmany() alone doesn't prevent. A plain cursor already streams on
        sqlite3 and pymssql.
        """

        return connection.cursor()


    def discardRemaining(self, connection: Any, cursor: Any) -> None:
        """Release rows left unread by an abandoned stream, so `connection` stays
        usable. Closing the cursor is enough everywhere but MySQL.
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
        """Whether the server reports this connection as encrypted in transit;
        None where there's no network or no way to tell.
        """

        return None

    def bulkInsert(self, cursor: Any, table: str, columns: List[str], rows: Sequence[Sequence[Any]]) -> bool:
        """Loads `rows` in fewer round trips than executemany, for drivers whose
        executemany sends a statement per row. False means nothing was sent.
        """

        return False

    def bulkUpsert(self, cursor: Any, table: str, allColumns: List[str], primaryKeyColumns: List[str], nonPrimaryKeyColumns: List[str],
                   rows: Sequence[Sequence[Any]]) -> bool:
        """bulkInsert for an upsert: the same contract. `rows` hold no two rows
        with the same key.
        """

        return False

    def truncateQuery(self, table: str) -> str:
        """TRUNCATE TABLE, which every dialect but SQLite has."""

        return 'TRUNCATE TABLE {}'.format(table)

    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """The category of a driver-specific cursor.description type_code, for
        discovery. None means unrecognized, and discovery samples values instead.
        """

        return None

    # The three catalog queries below each bind two parameters, the schema and
    # the table, from splitTableName. A NULL schema means the current one.

    def primaryKeyQuery(self) -> str:
        """One table's declared primary-key columns, in key order. Not UNIQUE
        constraints, which would make an upsert treat a changed row as new.
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
        """For planning subsets. SQLite, which can't do it in one query,
        overrides this.
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
    """PostgreSQL and SQLite share `INSERT ... ON CONFLICT` word for word. A
    key-only table gets DO NOTHING, since an empty SET is invalid.

    The stage form's `WHERE true` is SQLite's documented workaround for reading
    ON as the start of a join constraint.
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
        """Unbuffered, unlike connect()'s cursor. It holds the connection until
        drained: no other statement may run on it while a stream is open.
        """

        return connection.cursor(buffered=False)


    def discardRemaining(self, connection: Any, cursor: Any) -> None:
        """mysql.connector queues unread rows on the connection, where they fail
        the next statement ("Unread result found"). consume_results() reads and
        discards them -- bounded in memory, but it transfers every unread row.
        """

        connection.consume_results()


    def placeholders(self, count: int) -> List[str]:

        return count * ['%s']


    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """mysql.connector reports type names as strings, e.g. "VARCHAR"."""

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
        """A key-only table gets a no-op assignment of its key, since an empty
        SET is invalid. Not INSERT IGNORE, which also silences truncation,
        NOT NULL and foreign-key errors.
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

        # Only this schema, with no fallback such as `public` that catalog
        # lookups wouldn't see. Committed, or a later rollback would undo it.
        if settings.currentSchema:
            cursor.execute('SET search_path TO {}'.format(settings.currentSchema))
            connection.commit()

        return connection, cursor


    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:

        return {'user': settings.user, 'password': password, 'host': settings.host, 'database': settings.database,
                'port': settings.port}


    def streamingCursor(self, connection: Any, chunkSize: int) -> Any:
        """A named, server-side cursor: psycopg2 buffers everything through an
        unnamed one. A commit on the connection invalidates it, so the extract
        side never commits.
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
        """COPY into a temporary table, then one INSERT ... ON CONFLICT from it.
        The table empties at every commit, so each chunk reuses it.
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


    # The lookups fold names to lower case, as PostgreSQL does unquoted ones.
    # ::text gives a NULL schema the type lower() needs.

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
        """Renames, then recreates each view on the target from its definition
        captured beforehand, since a PostgreSQL view follows the table, not the
        name. CREATE OR REPLACE keeps grants and views built on it. See "How a
        swap works" in docs/design.md.
        """

        cursor.execute(self.DEPENDENT_VIEWS_QUERY, (targetTable,))
        views = cursor.fetchall()

        super().swap(cursor, targetTable, stageTable, tempTable)

        for name, definition in views:
            cursor.execute('CREATE OR REPLACE VIEW {} AS {}'.format(name, definition))


def _oracleLobsAsValues(cursor: Any, metadata: Any) -> Any:
    """Fetch CLOB, NCLOB and BLOB columns as str and bytes, not LOB handles,
    which no other driver can bind. Per connection, rather than oracledb's
    process-wide default, so an embedding application keeps its own setting.
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

    # ISO 8601 for implicit text-date conversions, so ISO text from other
    # databases, or a watermarkInitial, loads into a DATE. See "Moving values
    # between drivers" in docs/design.md.
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
        """A chunk per round trip rather than oracledb's default 100 rows.
        prefetchrows one above arraysize is oracledb's documented pairing.
        """

        cursor = connection.cursor()
        cursor.arraysize = chunkSize
        cursor.prefetchrows = chunkSize + 1

        return cursor


    def placeholders(self, count: int) -> List[str]:

        return [':{}'.format(i + 1) for i in range(count)]


    def columnCategory(self, dataType: Any) -> Optional[ColumnCategory]:
        """Matches oracledb's DB_TYPE_* objects by `.name`, so the driver isn't
        imported here.
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


    # all_* views filtered to the bound schema or the session's current one,
    # which user_* views wouldn't follow after ALTER SESSION SET CURRENT_SCHEMA.
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
        """Three statements, since cursor.execute() runs one. Not atomic:
        Oracle commits each DDL statement.
        """

        return _renameInThreeSteps(targetTable, stageTable, tempTable)


class MSSQLDialect(DatabaseDialect):

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import pymssql

        connection = pymssql.connect(**self.connectArguments(settings))
        cursor = connection.cursor()

        return connection, cursor


    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:
        """pymssql takes the port as a str, and fails on None, so it's left out
        when unset.
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
        """One execute(), inside the transaction, so atomic. sp_rename takes the
        new name literally, so it must be unqualified.
        """

        return ["EXEC sp_rename '{}', '{}'; EXEC sp_rename '{}', '{}'; EXEC sp_rename '{}', '{}';".format(
            stageTable, unqualifiedName(tempTable), targetTable, unqualifiedName(stageTable), tempTable, unqualifiedName(targetTable))]

    # No columnCategory: pymssql's type codes can't be told apart without
    # importing it, so discovery samples values instead.


class MariaDBDialect(MySQLDialect):
    """MySQL's dialect and driver, unchanged: MariaDB is compatible with
    everything this uses.
    """


def _registerSqliteAdapters(sqlite3: Any) -> None:
    """Teach sqlite3 the value types other drivers hand back: Decimal, which it
    refuses, and dates, whose built-in adapters are deprecated since 3.12.
    Process-wide, which is harmless for these.
    """

    import datetime
    import decimal

    sqlite3.register_adapter(decimal.Decimal, str)
    sqlite3.register_adapter(datetime.date, lambda value: value.isoformat())
    sqlite3.register_adapter(datetime.datetime, lambda value: value.isoformat(sep=' '))
    sqlite3.register_adapter(datetime.time, lambda value: value.isoformat())
    sqlite3.register_adapter(uuid.UUID, str)


class SQLiteDialect(_OnConflictDialect):
    """settings.database is a file path or ":memory:". No columnCategory:
    sqlite3 reports no column types.
    """

    def connect(self, settings: DatabaseConnectionConfig) -> Tuple[Any, Any]:

        import sqlite3

        _registerSqliteAdapters(sqlite3)

        # WAL, so a writer can proceed while a stream reads the same file; the
        # default journal fails it with "database is locked". It persists in
        # the file, and needs a local filesystem, not NFS or SMB.
        connection = sqlite3.connect(**self.connectArguments(settings))
        connection.execute('PRAGMA journal_mode=WAL')
        cursor = connection.cursor()

        return connection, cursor


    def _ownConnectArguments(self, settings: DatabaseConnectionConfig, password: Optional[str]) -> Dict[str, Any]:

        return {'database': settings.database, 'timeout': 30.0}


    def placeholders(self, count: int) -> List[str]:

        return count * ['?']


    def supportsMaterializedSelections(self) -> bool:
        """SQLite 3.35 and later, which not every Python links against."""

        import sqlite3

        return sqlite3.sqlite_version_info >= (3, 35)


    def truncateQuery(self, table: str) -> str:
        """SQLite has no TRUNCATE; DELETE FROM is its equivalent."""

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
        """Three statements in an explicit transaction: sqlite3 doesn't open
        one before DDL, so each rename would otherwise commit alone.
        """

        return ['BEGIN'] + _renameInThreeSteps(targetTable, stageTable, tempTable)
