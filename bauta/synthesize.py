"""Synthetic rows, for tables that can't be copied at all -- `bauta synthesize`.

Fills existing tables from nothing but the target's catalog. Integer keys
continue past the current maximum; foreign keys are drawn from parent rows,
so parents go first; columns named like personal data get what discovery's
proposed strategy makes of a placeholder; the rest is random within its type.
Deterministic for a seed and starting state.
"""
from __future__ import annotations

import datetime
import decimal
import hashlib
import uuid
from typing import Any, Callable, Dict, Iterator, List, NamedTuple, Optional, Sequence, Set, Tuple

from .databaseDialects import ColumnDefinition, ForeignKey, quoteIdentifier
from .discovery import NAME_RULES, nameWords
from .masking import STRATEGIES, KeyedHash
from .schema import INTEGER_BOOLEAN_NOTE, PortableType, portableType

DEFAULT_NULL_SHARE = 0.1

# How many parent keys are read to draw foreign-key values from.
PARENT_SAMPLE_SIZE = 100_000

_WORDS = ('alpha', 'bravo', 'delta', 'harbor', 'maple', 'orbit', 'quartz', 'river', 'summit', 'timber', 'velvet', 'willow',
          'amber', 'cobalt', 'ember', 'falcon', 'granite', 'juniper', 'lumen', 'meadow', 'nectar', 'pebble', 'saffron', 'tundra')

_EPOCH = datetime.date(2015, 1, 1)
_RECENT_DAYS = (datetime.date(2026, 12, 31) - _EPOCH).days


class SynthesisError(Exception):
    """A table that can't be filled as asked."""


class ColumnPlan(NamedTuple):
    """How one column is filled, and why -- what `--dry-run` prints."""

    column: str
    source: str
    description: str


Generator = Callable[[int], Any]


_FAKE_DESCRIPTIONS = {
    'fakeFirstName': 'a first name', 'fakeLastName': 'a last name', 'fakeName': 'a full name', 'fakeCity': 'a city',
    'fakeCompany': 'a company name', 'fakeStreetAddress': 'a street address',
    }


def _templateFor(words: Set[str]) -> str:

    if words & {'creditcard', 'cardnumber', 'ccnumber', 'pan'}:
        return '4000 0000 0000 0000'
    if words & {'zip', 'zipcode', 'postal', 'postalcode', 'postcode'}:
        return '00000'
    if words & {'ssn', 'socialsecurity', 'socialsecuritynumber'}:
        return '000-00-0000'
    if words & {'phone', 'phonenumber', 'mobile', 'cell', 'fax', 'telephone', 'tel'}:
        return '+1 555 010 0000'

    return '000000000000'


class _Synthesizer:
    """Builds one table's column generators."""

    def __init__(self, table: str, seed: int, nullShare: float) -> None:
        self.salt = 'bauta synthetic data|{}|{}'.format(seed, table.lower())
        self.keyedHash = KeyedHash(self.salt, table.lower())
        self.nullShare = nullShare


    def _unit(self, rowNumber: int, column: str) -> float:
        """A number in [0, 1) that depends only on the seed, table, column and row,
        so the same seed makes the same rows.
        """

        digest = hashlib.sha256('{}|{}|{}'.format(self.salt, column.lower(), rowNumber).encode()).digest()

        return int.from_bytes(digest[:7], 'big') / float(1 << 56)


    def byName(self, column: ColumnDefinition, portable: PortableType) -> Optional[Tuple[Generator, str]]:
        """A realistic generator where the column's name suggests personal data."""

        words = nameWords(column.name)
        textual = portable.kind in ('text', 'fixedText')
        numeric = portable.kind in ('smallint', 'integer', 'bigint', 'decimal', 'float')
        dated = portable.kind in ('date', 'timestamp', 'timestampTz')

        for ruleWords, policy, _ in NAME_RULES:
            if not words & set(ruleWords):
                continue
            strategy = policy['strategy']
            name = column.name

            if strategy == 'email' and textual:
                email = STRATEGIES['email'](self.keyedHash, {})
                return (lambda row: email.mask('synthetic-{}@example.test'.format(row))), 'an email address at example.test'
            if strategy.startswith('fake') and textual:
                fake = STRATEGIES[strategy](self.keyedHash, {})
                return (lambda row: fake.mask(row)), _FAKE_DESCRIPTIONS[strategy]
            if strategy in ('digits', 'key') and (textual or numeric):
                if strategy == 'key' and not words & {'ssn', 'socialsecurity', 'socialsecuritynumber'}:
                    return (lambda row: 'user{}'.format(row)), 'a user name'
                template = _templateFor(words)
                return self._digitsLike(template, numeric), 'digits shaped like {}'.format(template)
            if strategy == 'hash' and textual:
                hashed = STRATEGIES['hash'](self.keyedHash, {})
                return (lambda row: hashed.mask('synthetic {} {}'.format(name, row))), 'an opaque token'
            if strategy == 'dateShift' and dated:
                return (lambda row: datetime.date(1940, 1, 1) + datetime.timedelta(days=int(self._unit(row, name) * 23725))), \
                    'a birth date between 1940 and 2004'
            if strategy == 'number' and numeric:
                if words & {'latitude', 'lat'}:
                    return (lambda row: round(self._unit(row, name) * 180 - 90, 5)), 'a latitude'
                if words & {'longitude', 'lng', 'lon'}:
                    return (lambda row: round(self._unit(row, name) * 360 - 180, 5)), 'a longitude'
                return (lambda row: 30000 + int(self._unit(row, name) * 170000)), 'an amount between 30,000 and 200,000'
            if strategy == 'shuffle' and textual:
                choices = ('F', 'M', 'X') if words & {'gender', 'sex'} else ('A', 'B', 'C', 'D')
                return (lambda row: choices[int(self._unit(row, name) * len(choices))]), 'one of {}'.format(', '.join(choices))
            if strategy == 'null' and textual:
                return (lambda row: self._sentence(row, name)), 'words'

        return None


    def _digitsLike(self, template: str, numeric: bool) -> Generator:
        """Values shaped like `template` -- a phone number, a postal code --
        with its digits generated; as an integer for a numeric column.
        """

        digits = STRATEGIES['digits'](self.keyedHash, {})
        count = sum(character.isdigit() for character in template)

        def generate(row: int) -> Any:
            # The row number, in the template's digits, keyed into other digits.
            seeded = iter(str(row).rjust(count, '0')[-count:])
            text = digits.mask(''.join(next(seeded) if character.isdigit() else character for character in template))
            if not numeric:
                return text
            number = ''.join(character for character in text if character.isdigit())[:9]
            return int(number.lstrip('0') or '0')

        return generate


    def _sentence(self, row: int, column: str) -> str:

        count = 3 + int(self._unit(row, column + '#count') * 8)

        return ' '.join(_WORDS[int(self._unit(row, '{}#{}'.format(column, index)) * len(_WORDS))] for index in range(count)).capitalize() + '.'


    def byType(self, column: ColumnDefinition, portable: PortableType) -> Tuple[Generator, str]:
        """A random value of the column's type, within its size."""

        name = column.name
        unit = self._unit
        kind = portable.kind

        if portable.note == INTEGER_BOOLEAN_NOTE:
            return (lambda row: int(unit(row, name) < 0.5)), '0 or 1, a boolean stored as an integer'
        if kind in ('smallint', 'integer', 'bigint'):
            # Within the declared precision where there is one: Oracle's
            # NUMBER(1) takes 0-9. Catalogs that count precision in bits give
            # a looser bound, never a wrong one.
            ceiling = {'smallint': 100, 'integer': 100_000, 'bigint': 1_000_000_000}[kind]
            if column.precision:
                ceiling = min(ceiling, 10 ** column.precision)
            return (lambda row: int(unit(row, name) * ceiling)), 'an integer below {:,}'.format(ceiling)
        if kind == 'decimal':
            scale = portable.scale or 0
            wholeDigits = min(6, (portable.precision or 12) - scale)
            step = decimal.Decimal(1).scaleb(-scale)
            ceiling = 10 ** max(0, wholeDigits)
            return (lambda row: (decimal.Decimal(repr(unit(row, name))) * ceiling).quantize(step, rounding=decimal.ROUND_DOWN)), \
                'a decimal with {} places'.format(scale)
        if kind == 'float':
            return (lambda row: round(unit(row, name) * 1000, 3)), 'a number'
        if kind == 'boolean':
            return (lambda row: unit(row, name) < 0.5), 'true or false'
        if kind == 'fixedText':
            width = portable.length or 1
            return (lambda row: ''.join(chr(65 + int(unit(row, '{}#{}'.format(name, index)) * 26)) for index in range(width))), \
                '{} letters'.format(width)
        if kind == 'text':
            limit = portable.length
            return (lambda row: self._sentence(row, name)), 'words' + (', at most {} characters'.format(limit) if limit else '')
        if kind == 'date':
            return (lambda row: _EPOCH + datetime.timedelta(days=int(unit(row, name) * _RECENT_DAYS))), 'a date since 2015'
        if kind in ('timestamp', 'timestampTz'):
            zone = datetime.timezone.utc if kind == 'timestampTz' else None
            start = datetime.datetime.combine(_EPOCH, datetime.time(), zone)
            return (lambda row: start + datetime.timedelta(seconds=int(unit(row, name) * _RECENT_DAYS * 86400))), 'a timestamp since 2015'
        if kind == 'time':
            return (lambda row: (datetime.datetime.min + datetime.timedelta(seconds=int(unit(row, name) * 86400))).time()), 'a time of day'
        if kind == 'binary':
            size = min(16, portable.length or 16)
            return (lambda row: hashlib.sha256('{}|{}'.format(name, row).encode()).digest()[:size]), '{} random bytes'.format(size)
        if kind == 'uuid':
            return (lambda row: str(uuid.UUID(bytes=hashlib.sha256('{}|{}'.format(name, row).encode()).digest()[:16], version=4))), 'a UUID'
        if kind == 'json':
            return (lambda row: '{{"synthetic": true, "row": {}}}'.format(row)), 'a small JSON document'

        return (lambda row: self._sentence(row, name)), 'words'


    def orNull(self, generator: Generator, column: ColumnDefinition) -> Generator:
        """NULL for a share of rows, where the column allows it."""

        if not column.nullable or self.nullShare <= 0:
            return generator

        return lambda row: None if self._unit(row, column.name + '#null') < self.nullShare else generator(row)


def _sequential(start: int) -> Generator:

    return lambda row: start + row


def _offset(generator: Generator, offset: int) -> Generator:

    return lambda row: generator(offset + row)


def _textKeys(existing: int, width: int, fixed: bool) -> Generator:
    """S1, S2, ... -- or, for a fixed-width column, S0001, S0002, ..., padded
    between the S and the number, so every key is distinct at full width.
    """

    if fixed:
        return lambda row: 'S' + str(existing + row + 1).zfill(width - 1)

    return lambda row: 'S{}'.format(existing + row + 1)


def _integerKind(portable: PortableType) -> bool:

    return portable.kind in ('smallint', 'integer', 'bigint') or (portable.kind == 'decimal' and not portable.scale)


def planTable(database: Any, table: str, rows: int, seed: int = 0, foreignKeys: Optional[Sequence[ForeignKey]] = None,
              nullShare: float = DEFAULT_NULL_SHARE) -> Tuple[List[str], Callable[[int], Tuple[Any, ...]], List[ColumnPlan], int]:
    """How `table` would be filled: its columns, a row generator, what each
    column gets, and how many rows can be made.

    Reads the target's catalog and, for foreign keys, the parent tables' keys;
    writes nothing.
    """

    definitions = database.getColumnDefinitions(table)
    if not definitions:
        raise SynthesisError('table {} was not found'.format(table))

    primaryKey = {column.upper() for column in database.getPrimaryColumnNames(table)}
    foreignKeys = [foreignKey for foreignKey in (foreignKeys if foreignKeys is not None else database.getForeignKeys())
                   if foreignKey.table.upper() == table.split('.')[-1].upper()]
    synthesizer = _Synthesizer(table, seed, nullShare)
    spelled = {definition.name.upper(): definition.name for definition in definitions}
    generators: Dict[str, Generator] = {}
    plans: Dict[str, ColumnPlan] = {}
    available = rows

    for foreignKey in foreignKeys:
        columns = [column.upper() for column in foreignKey.columns]
        selfReference = foreignKey.referencedTable.upper() == foreignKey.table.upper()
        nullable = all(definition.nullable for definition in definitions if definition.name.upper() in columns)
        parentKeys: List[Tuple[Any, ...]] = []

        if not selfReference:
            query = 'SELECT DISTINCT {} FROM {} WHERE {}'.format(
                ', '.join(foreignKey.referencedColumns), foreignKey.referencedTable,
                ' AND '.join('{} IS NOT NULL'.format(column) for column in foreignKey.referencedColumns))
            _, parentKeys = database.sample(query, PARENT_SAMPLE_SIZE)
            parentKeys = [tuple(key) for key in parentKeys]
            if not parentKeys and not nullable:
                raise SynthesisError('{} references {}, which has no rows; fill {} first'.format(table, foreignKey.referencedTable,
                                                                                               foreignKey.referencedTable))

        for position, column in enumerate(columns):
            def pick(row: int, keys: List[Tuple[Any, ...]] = parentKeys, index: int = position, name: str = foreignKey.name) -> Any:
                if not keys:
                    return None
                return keys[int(synthesizer._unit(row, name) * len(keys))][index]
            generators[column] = pick
            description = 'NULL, as {} has no rows'.format(foreignKey.referencedTable) if not parentKeys and not selfReference \
                else 'an existing {} key'.format(foreignKey.referencedTable)
            if selfReference:
                description = 'NULL (references its own table)'
                generators[column] = lambda row: None
            plans[column] = ColumnPlan(spelled.get(column, column), 'foreign key', description)

        if selfReference and not nullable:
            raise SynthesisError('{} references itself through NOT NULL column(s) {}; synthesize can only leave such references NULL'.format(
                table, ', '.join(foreignKey.columns)))

    keyColumns = [definition for definition in definitions if definition.name.upper() in primaryKey]
    generatedKeyParts = [definition for definition in keyColumns if definition.name.upper() not in generators]

    for definition in generatedKeyParts:
        portable = portableType(database.type, definition)
        name = definition.name
        if _integerKind(portable):
            current = database.query('SELECT max({}) FROM {}'.format(quoteIdentifier(database.type, name), table))[0][0]
            start = int(current or 0) + 1
            generators[name.upper()] = _sequential(start)
            plans[name.upper()] = ColumnPlan(name, 'primary key', 'sequential, from {}'.format(start))
        elif portable.kind == 'uuid':
            # Offset by the rows already there, so a second run doesn't repeat the first's keys.
            existing = int(database.query('SELECT count(*) FROM {}'.format(table))[0][0])
            generator, _ = synthesizer.byType(definition, portable)
            generators[name.upper()] = _offset(generator, existing)
            plans[name.upper()] = ColumnPlan(name, 'primary key', 'a unique UUID')
        elif portable.kind in ('text', 'fixedText'):
            existing = int(database.query('SELECT count(*) FROM {}'.format(table))[0][0])
            width = portable.length or 12
            if len('S{}'.format(existing + rows)) > width:
                raise SynthesisError('{}.{} holds only {} characters, too few for {} unique keys'.format(table, name, width, rows))
            generators[name.upper()] = _textKeys(existing, width, portable.kind == 'fixedText')
            plans[name.upper()] = ColumnPlan(name, 'primary key', 'unique text, {} onwards'.format(generators[name.upper()](0)))
        else:
            raise SynthesisError('{}.{} is a {} primary key, which synthesize can\'t make unique'.format(table, name, portable.kind))

    if keyColumns and not generatedKeyParts:
        # Every key column is a foreign key -- a bridge table. Only as many
        # distinct combinations exist as the parents allow.
        available = min(rows, _combinations(generators, [definition.name.upper() for definition in keyColumns]))

    for definition in definitions:
        name = definition.name.upper()
        if name in generators:
            continue
        portable = portableType(database.type, definition)
        named = synthesizer.byName(definition, portable)
        if named is not None:
            generator, reason = named
            plans[name] = ColumnPlan(definition.name, 'name', reason)
        else:
            generator, description = synthesizer.byType(definition, portable)
            plans[name] = ColumnPlan(definition.name, 'type', description + (', sometimes NULL' if definition.nullable else ''))
        generators[name] = synthesizer.orNull(_fitting(generator, definition.length), definition)

    columns = [definition.name for definition in definitions]
    ordered = [generators[column.upper()] for column in columns]

    def makeRow(row: int) -> Tuple[Any, ...]:
        return tuple(generator(row) for generator in ordered)

    return columns, makeRow, [plans[column.upper()] for column in columns], available


def _fitting(generator: Generator, length: Optional[int]) -> Generator:
    """Text cut to the column's length; other values as they are."""

    if not length or length < 0:
        return generator

    def fitted(row: int) -> Any:
        value = generator(row)
        return value[:length] if isinstance(value, str) else value

    return fitted


def _combinations(generators: Dict[str, Generator], keyColumns: List[str]) -> int:
    """How many distinct key tuples the foreign-key generators can make, by trying."""

    seen = {tuple(generators[column](row) for column in keyColumns) for row in range(PARENT_SAMPLE_SIZE)}

    return len(seen)


def synthesizeTable(database: Any, table: str, rows: int, seed: int = 0, foreignKeys: Optional[Sequence[ForeignKey]] = None,
                    chunkSize: int = 1000, nullShare: float = DEFAULT_NULL_SHARE) -> int:
    """Inserts up to `rows` generated rows into `table`, and returns how many.

    Fewer than asked only for a table whose primary key is made entirely of
    foreign keys, which can't have more distinct rows than its parents allow.
    """

    columns, makeRow, _, available = planTable(database, table, rows, seed=seed, foreignKeys=foreignKeys, nullShare=nullShare)
    keyColumns = {column.upper() for column in database.getPrimaryColumnNames(table)}
    keyIndexes = [index for index, column in enumerate(columns) if column.upper() in keyColumns]
    seen: Set[Tuple[Any, ...]] = set()
    inserted = 0

    for chunk in _chunks(makeRow, rows, available, keyIndexes, seen, chunkSize):
        database.insert(table=table, data=chunk, chunkSize=chunkSize, columns=columns)
        inserted += len(chunk)

    return inserted


def _chunks(makeRow: Callable[[int], Tuple[Any, ...]], rows: int, available: int, keyIndexes: List[int], seen: Set[Tuple[Any, ...]],
            chunkSize: int) -> Iterator[List[Tuple[Any, ...]]]:
    """Generated rows, a chunk at a time, skipping repeated keys -- which only a
    key made wholly of foreign keys can produce.
    """

    chunk: List[Tuple[Any, ...]] = []
    produced = 0
    row = 0
    attempts = 0

    while produced < available and attempts < max(rows, available) * 20:
        values = makeRow(row)
        row += 1
        attempts += 1
        key = tuple(values[index] for index in keyIndexes)
        if keyIndexes and key in seen:
            continue
        seen.add(key)
        chunk.append(values)
        produced += 1
        if len(chunk) == chunkSize:
            yield chunk
            chunk = []

    if chunk:
        yield chunk
