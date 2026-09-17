"""Creates target tables from source ones, and empties targets before a refresh.

`understudy schema` reads a table's columns, primary key and foreign keys
from the source catalog and writes CREATE TABLE statements in the *target's*
dialect. Types go through a small portable vocabulary on the way -- integer,
decimal, text, timestamp and so on -- because six dialects can't be mapped
pairwise. Anything the vocabulary can't express becomes text, and the
statement says so in a comment rather than failing, since one odd column
shouldn't block creating the rest.

This is deliberately table *shape* only: columns, nullability, the primary key
and foreign keys. Indexes, defaults, check constraints, triggers and
permissions aren't copied -- a non-production copy rarely needs them, and
translating them across dialects is where schema tools go wrong.

`understudy clear` empties the target tables of a set of jobs, children
before parents so foreign keys don't block it.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, Set, Tuple

from .configuration import DatabaseType
from .databaseDialects import ColumnDefinition, ForeignKey, quoteFolded


class SchemaError(Exception):
    """A schema that can't be generated or cleared as asked."""


class PortableType(NamedTuple):
    """A column type in terms every dialect can render.

    kind is one of: smallint, integer, bigint, decimal, float, boolean, text,
    fixedText, date, timestamp, timestampTz, time, binary, uuid, json.
    """

    kind: str
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    note: Optional[str] = None


class TableDefinition(NamedTuple):

    name: str
    columns: List[ColumnDefinition]
    primaryKey: List[str]
    foreignKeys: List[ForeignKey]


class Statement(NamedTuple):
    """One DDL statement, with any notes about lossy type choices."""

    table: str
    sql: str
    notes: List[str]


INTEGER_BOOLEAN_NOTE = 'the source stores this as an integer, so it stays one'


def _integerForPrecision(precision: Optional[int]) -> PortableType:

    if precision is None:
        return PortableType('bigint', note='unconstrained integer; mapped to a 64-bit integer')
    if precision <= 4:
        return PortableType('smallint')
    if precision <= 9:
        return PortableType('integer')
    if precision <= 18:
        return PortableType('bigint')

    return PortableType('decimal', precision=precision, scale=0)


def _text(length: Optional[int], fixed: bool = False) -> PortableType:
    """Unbounded when the catalog says so: None, or -1 for SQL Server's (MAX)."""

    if length is None or length < 0:
        return PortableType('text')

    return PortableType('fixedText' if fixed else 'text', length=length)


def portableType(sourceType: DatabaseType, column: ColumnDefinition) -> PortableType:
    """Maps one source column to the portable vocabulary."""

    name = column.dataType.lower().strip()
    base = re.sub(r'\(.*?\)', '', name).strip()

    if sourceType == DatabaseType.ORACLE:
        if base == 'number':
            # INTEGER is stored as NUMBER with scale 0 and no precision.
            if column.scale == 0:
                return _integerForPrecision(column.precision)
            if column.precision is None:
                return PortableType('decimal', note='unconstrained NUMBER; mapped to an unbounded decimal')
            return PortableType('decimal', precision=column.precision, scale=column.scale)
        if base in ('float', 'binary_double', 'binary_float'):
            return PortableType('float')
        if base in ('varchar2', 'nvarchar2', 'varchar'):
            return _text(column.length)
        if base in ('char', 'nchar'):
            return _text(column.length, fixed=True)
        if base in ('clob', 'nclob', 'long'):
            return PortableType('text')
        if base == 'date':
            return PortableType('timestamp', note='Oracle DATE carries a time of day; mapped to a timestamp')
        if base.startswith('timestamp'):
            return PortableType('timestampTz' if 'time zone' in base else 'timestamp')
        if base in ('blob', 'raw', 'long raw'):
            return PortableType('binary')
        return PortableType('text', note='unrecognized Oracle type {}; mapped to text'.format(column.dataType))

    if sourceType == DatabaseType.SQLITE:
        # SQLite's own type-affinity rules, in its documented order.
        upper = base.upper()
        if 'INT' in upper:
            return PortableType('bigint') if 'BIG' in upper else PortableType('integer')
        if 'BOOL' in upper:
            return PortableType('smallint', note=INTEGER_BOOLEAN_NOTE)
        if any(word in upper for word in ('CHAR', 'CLOB', 'TEXT')):
            return _text(column.length, fixed=upper in ('CHAR', 'NCHAR'))
        if 'BLOB' in upper or not upper:
            return PortableType('binary') if upper else PortableType('text', note='no declared type; mapped to text')
        if any(word in upper for word in ('REAL', 'FLOA', 'DOUB')):
            return PortableType('float')
        if 'DATETIME' in upper or 'TIMESTAMP' in upper:
            return PortableType('timestamp')
        if 'DATE' in upper:
            return PortableType('date')
        if 'TIME' in upper:
            return PortableType('time')
        if any(word in upper for word in ('DEC', 'NUM')):
            return PortableType('decimal', precision=column.precision, scale=column.scale)
        return PortableType('text', note='unrecognized SQLite type {}; mapped to text'.format(column.dataType))

    # MySQL, MariaDB, PostgreSQL and SQL Server all report through
    # information_schema, with names that overlap enough to share one table.
    if base in ('tinyint', 'smallint', 'int2'):
        return PortableType('smallint')
    if base in ('int', 'integer', 'mediumint', 'int4', 'serial'):
        return PortableType('integer')
    if base in ('bigint', 'int8', 'bigserial'):
        return PortableType('bigint')
    if base in ('decimal', 'numeric', 'money', 'smallmoney'):
        if base in ('money', 'smallmoney'):
            return PortableType('decimal', precision=19, scale=4)
        return PortableType('decimal', precision=column.precision, scale=column.scale)
    if base in ('float', 'double', 'double precision', 'real', 'float4', 'float8'):
        return PortableType('float')
    # Only these two drivers hand back real booleans. MySQL's BOOLEAN is a
    # TINYINT and its BIT(n) a bit string, and both arrive as integers, which
    # PostgreSQL would refuse to load into a BOOLEAN column.
    if (sourceType == DatabaseType.POSTGRESQL and base in ('boolean', 'bool')) or (sourceType == DatabaseType.MSSQL and base == 'bit'):
        return PortableType('boolean')
    if base in ('bit', 'boolean', 'bool'):
        return PortableType('bigint' if base == 'bit' else 'smallint', note=INTEGER_BOOLEAN_NOTE)
    if base in ('varchar', 'nvarchar', 'character varying', 'varchar2'):
        return _text(column.length)
    if base in ('char', 'nchar', 'character', 'bpchar'):
        return _text(column.length, fixed=True)
    if base in ('text', 'ntext', 'tinytext', 'mediumtext', 'longtext', 'citext', 'xml', 'enum', 'set'):
        # MySQL reports TEXT's 65535-byte limit as a length; it isn't one in
        # characters, and the type is unbounded for any practical purpose.
        note = 'MySQL {} values; mapped to text'.format(base.upper()) if base in ('enum', 'set') else None
        return PortableType('text', note=note)
    if base == 'date':
        return PortableType('date')
    if base in ('datetime', 'datetime2', 'smalldatetime', 'timestamp', 'timestamp without time zone'):
        return PortableType('timestamp')
    if base in ('datetimeoffset', 'timestamp with time zone', 'timestamptz'):
        return PortableType('timestampTz')
    if base in ('time', 'time without time zone'):
        return PortableType('time')
    if base in ('binary', 'varbinary', 'blob', 'tinyblob', 'mediumblob', 'longblob', 'bytea', 'image'):
        return PortableType('binary')
    if base in ('uuid', 'uniqueidentifier'):
        return PortableType('uuid')
    if base in ('json', 'jsonb'):
        return PortableType('json')

    return PortableType('text', note='unrecognized type {}; mapped to text'.format(column.dataType))


# Where a key column can't be an unbounded type -- MySQL can't index TEXT,
# SQL Server can't index NVARCHAR(MAX), Oracle can't index a CLOB -- it gets
# this bounded length instead.
KEY_TEXT_LENGTH = 255


def renderType(targetType: DatabaseType, portable: PortableType, isKey: bool) -> Tuple[str, Optional[str]]:
    """The target dialect's type for a portable one, and a note if it's lossy."""

    kind, length, precision, scale = portable.kind, portable.length, portable.precision, portable.scale
    note = None

    if kind == 'text' and length is None and isKey and targetType != DatabaseType.POSTGRESQL and targetType != DatabaseType.SQLITE:
        length = KEY_TEXT_LENGTH
        note = 'unbounded text in a key; bounded to {} characters'.format(KEY_TEXT_LENGTH)

    if targetType in (DatabaseType.MYSQL, DatabaseType.MARIADB):
        if kind == 'decimal':
            return ('DECIMAL({},{})'.format(min(precision, 65), min(scale or 0, 30)) if precision else 'DECIMAL(65,30)'), note
        if kind == 'text' and (length is None or length > 16383):
            return 'LONGTEXT', note
        rendered = {
            'smallint': 'SMALLINT', 'integer': 'INT', 'bigint': 'BIGINT', 'float': 'DOUBLE', 'boolean': 'BOOLEAN',
            'text': 'VARCHAR({})'.format(length), 'fixedText': 'CHAR({})'.format(length), 'date': 'DATE',
            'timestamp': 'DATETIME(6)', 'timestampTz': 'DATETIME(6)', 'time': 'TIME(6)', 'binary': 'LONGBLOB',
            'uuid': 'CHAR(36)', 'json': 'JSON',
            }[kind]
        if kind == 'timestampTz':
            note = 'MySQL has no time-zone-aware timestamp; the offset is not kept'
        return rendered, note

    if targetType == DatabaseType.POSTGRESQL:
        if kind == 'decimal':
            return ('NUMERIC({},{})'.format(precision, scale or 0) if precision else 'NUMERIC'), note
        return {
            'smallint': 'SMALLINT', 'integer': 'INTEGER', 'bigint': 'BIGINT', 'float': 'DOUBLE PRECISION', 'boolean': 'BOOLEAN',
            'text': 'VARCHAR({})'.format(length) if length else 'TEXT', 'fixedText': 'CHAR({})'.format(length), 'date': 'DATE',
            'timestamp': 'TIMESTAMP', 'timestampTz': 'TIMESTAMPTZ', 'time': 'TIME', 'binary': 'BYTEA', 'uuid': 'UUID', 'json': 'JSONB',
            }[kind], note

    if targetType == DatabaseType.MSSQL:
        if kind == 'decimal':
            return ('DECIMAL({},{})'.format(min(precision, 38), min(scale or 0, 38)) if precision else 'DECIMAL(38,10)'), note
        if kind in ('text', 'json') and (length is None or length > 4000):
            return 'NVARCHAR(MAX)', note
        return {
            'smallint': 'SMALLINT', 'integer': 'INT', 'bigint': 'BIGINT', 'float': 'FLOAT', 'boolean': 'BIT',
            'text': 'NVARCHAR({})'.format(length), 'fixedText': 'NCHAR({})'.format(length), 'date': 'DATE',
            'timestamp': 'DATETIME2', 'timestampTz': 'DATETIMEOFFSET', 'time': 'TIME', 'binary': 'VARBINARY(MAX)',
            'uuid': 'UNIQUEIDENTIFIER',
            }[kind], note

    if targetType == DatabaseType.ORACLE:
        if kind == 'decimal':
            return ('NUMBER({},{})'.format(min(precision, 38), scale or 0) if precision else 'NUMBER'), note
        if kind in ('text', 'json') and (length is None or length > 4000):
            return 'CLOB', note
        if kind == 'time':
            return 'VARCHAR2(16 CHAR)', 'Oracle has no TIME type; mapped to text'
        if kind == 'boolean':
            return 'NUMBER(1)', 'mapped to NUMBER(1)'
        return {
            'smallint': 'NUMBER(5)', 'integer': 'NUMBER(10)', 'bigint': 'NUMBER(19)', 'float': 'BINARY_DOUBLE',
            'text': 'VARCHAR2({} CHAR)'.format(length), 'fixedText': 'CHAR({} CHAR)'.format(length), 'date': 'DATE',
            'timestamp': 'TIMESTAMP', 'timestampTz': 'TIMESTAMP WITH TIME ZONE', 'binary': 'BLOB', 'uuid': 'VARCHAR2(36 CHAR)',
            }[kind], note

    # SQLite: declared names that give each value the right affinity.
    if kind == 'decimal':
        return ('DECIMAL({},{})'.format(precision, scale or 0) if precision else 'NUMERIC'), note
    return {
        'smallint': 'SMALLINT', 'integer': 'INTEGER', 'bigint': 'BIGINT', 'float': 'REAL', 'boolean': 'BOOLEAN',
        'text': 'VARCHAR({})'.format(length) if length else 'TEXT', 'fixedText': 'CHAR({})'.format(length), 'date': 'DATE',
        'timestamp': 'TIMESTAMP', 'timestampTz': 'TIMESTAMP', 'time': 'TIME', 'binary': 'BLOB', 'uuid': 'VARCHAR(36)', 'json': 'TEXT',
        }[kind], note


def readTable(database: Any, table: str, foreignKeys: Sequence[ForeignKey]) -> TableDefinition:
    """A table's shape from a live source Database."""

    columns = database.getColumnDefinitions(table)
    if not columns:
        raise SchemaError('table {} was not found in the source database'.format(table))

    name = next((foreignKey.table for foreignKey in foreignKeys if foreignKey.table.upper() == table.upper()), None) \
        or next((foreignKey.referencedTable for foreignKey in foreignKeys if foreignKey.referencedTable.upper() == table.upper()), table)

    return TableDefinition(name=name, columns=columns, primaryKey=database.getPrimaryColumnNames(table),
                           foreignKeys=[foreignKey for foreignKey in foreignKeys if foreignKey.table.upper() == table.upper()])


def orderParentsFirst(tables: Iterable[str], foreignKeys: Sequence[ForeignKey]) -> List[str]:
    """Tables ordered so each comes after every table it references.

    Self-references don't constrain the order. A cycle between tables can't be
    ordered at all, and raises.
    """

    byName = {table.upper(): table for table in tables}
    parents: Dict[str, Set[str]] = {name: set() for name in byName}

    for foreignKey in foreignKeys:
        child, parent = foreignKey.table.upper(), foreignKey.referencedTable.upper()
        if child in byName and parent in byName and child != parent:
            parents[child].add(parent)

    ordered: List[str] = []
    remaining = set(byName)

    while remaining:
        ready = sorted(name for name in remaining if not parents[name] & remaining)
        if not ready:
            raise SchemaError('foreign keys form a cycle among: {}. Leave the foreign keys out (--no-foreign-keys), '
                              'or add them yourself once both tables exist'.format(', '.join(sorted(byName[name] for name in remaining))))
        ordered += ready
        remaining -= set(ready)

    return [byName[name] for name in ordered]


def createStatements(sourceType: DatabaseType, targetType: DatabaseType, tables: Sequence[TableDefinition],
                     includeForeignKeys: bool = True, stageSuffix: Optional[str] = None, stagesOnly: bool = False) -> List[Statement]:
    """CREATE TABLE statements for `tables`, parents first.

    Foreign keys are declared inline, and only between tables in the set: a
    reference to a table that isn't being created is left out and noted, since
    the target may not have it. Stage tables -- `<table><stageSuffix>`, what a
    swap loads into -- get the same columns and primary key but no foreign
    keys, because a stage table is emptied and swapped, and nothing should
    reference it.
    """

    order = orderParentsFirst([table.name for table in tables], [foreignKey for table in tables for foreignKey in table.foreignKeys]
                              if includeForeignKeys else [])
    byName = {table.name.upper(): table for table in tables}
    names = {table.name.upper() for table in tables}
    statements = []

    for name in order:
        table = byName[name.upper()]
        if not stagesOnly:
            statements.append(_createTable(sourceType, targetType, table, table.name, includeForeignKeys, names))
        if stageSuffix:
            statements.append(_createTable(sourceType, targetType, table, table.name + stageSuffix, False, names))

    return statements


def _createTable(sourceType: DatabaseType, targetType: DatabaseType, table: TableDefinition, name: str,
                 includeForeignKeys: bool, created: Set[str]) -> Statement:

    keyColumns = {column.upper() for column in table.primaryKey}
    for foreignKey in table.foreignKeys:
        keyColumns.update(column.upper() for column in foreignKey.columns)

    lines = []
    notes = []

    def quoted(names: Iterable[str]) -> str:
        return ', '.join(quoteFolded(targetType, name) for name in names)

    for column in table.columns:
        portable = portableType(sourceType, column)
        rendered, renderNote = renderType(targetType, portable, column.name.upper() in keyColumns)
        nullable = column.nullable and column.name.upper() not in {key.upper() for key in table.primaryKey}
        lines.append('{} {}{}'.format(quoteFolded(targetType, column.name), rendered, '' if nullable else ' NOT NULL'))
        for note in (portable.note, renderNote):
            if note:
                notes.append('{}: {}'.format(column.name, note))

    if table.primaryKey:
        lines.append('PRIMARY KEY ({})'.format(quoted(table.primaryKey)))

    if includeForeignKeys:
        for foreignKey in table.foreignKeys:
            if foreignKey.referencedTable.upper() not in created:
                notes.append('foreign key {} -> {} left out: {} is not being created'.format(
                    ', '.join(foreignKey.columns), foreignKey.referencedTable, foreignKey.referencedTable))
                continue
            lines.append('CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({})'.format(
                _constraintName(targetType, foreignKey.name), quoted(foreignKey.columns), foreignKey.referencedTable,
                quoted(foreignKey.referencedColumns)))

    sql = 'CREATE TABLE {} (\n    {}\n)'.format(name, ',\n    '.join(lines))

    return Statement(table=name, sql=sql, notes=notes)


def _constraintName(targetType: DatabaseType, name: str) -> str:
    """Source constraint names, kept where the target accepts them.

    Names are only guaranteed unique within their own source schema, and
    PostgreSQL's system-generated ones can contain characters that need
    quoting, so anything outside [A-Za-z0-9_] is replaced, and the result is
    kept within the shortest identifier limit among the dialects (63).
    """

    cleaned = re.sub(r'[^A-Za-z0-9_]', '_', name)
    if not re.match(r'[A-Za-z]', cleaned):
        cleaned = 'fk_' + cleaned

    return cleaned[:63 if targetType != DatabaseType.MYSQL else 64]


def renderScript(statements: Sequence[Statement], heading: Sequence[str]) -> str:
    """The statements as one SQL script, each note as a comment above its table."""

    parts = ['\n'.join('-- ' + line if line else '--' for line in heading)] if heading else []

    for statement in statements:
        comments = ''.join('-- {}\n'.format(note) for note in statement.notes)
        parts.append('{}{};'.format(comments, statement.sql))

    return '\n\n'.join(parts) + '\n'


def clearOrder(tables: Iterable[str], foreignKeys: Sequence[ForeignKey]) -> List[str]:
    """Tables ordered so each is emptied before the tables it references."""

    return list(reversed(orderParentsFirst(tables, foreignKeys)))


def clearTables(database: Any, tables: Sequence[str]) -> List[Tuple[str, int]]:
    """Deletes every row of `tables`, in one transaction, children first.

    DELETE rather than TRUNCATE: PostgreSQL, SQL Server and Oracle refuse to
    truncate a table that a foreign key references, even when the referencing
    table is empty. One transaction, so a failure part-way -- a table outside
    the set still referencing these rows, say -- leaves every table as it was.

    Returns (table, rows deleted) in the order they were emptied.
    """

    order = clearOrder(tables, database.getForeignKeys())
    cleared = []

    try:
        for table in order:
            database.cursor.execute('DELETE FROM {}'.format(table))
            cleared.append((table, database.cursor.rowcount))
        database.connection.commit()
    except Exception:
        database.connection.rollback()
        raise

    return cleared
