"""Deterministic, keyed masking, applied to rows on their way to the target.

Every masked value is derived from a keyed hash of the value itself, scoped by a
named *domain*:

    mask = strategy( HMAC(key, domain, value) )

That one construction is what the three properties people actually need from
masking hang off:

- Referential consistency. The same value in the same domain masks the same way
  in every table, so `orders.customerId` and `customers.id` still join when both
  use `domain: customer`.
- Reproducibility. Nothing depends on row order or an unseeded random source, so
  a re-run -- or an incremental run next week -- produces the same masks.
- Irreversibility without the key. The key is what stops someone who knows the
  scheme from hashing candidate values and matching them up.

Nothing here imports the rest of the package: a policy is plain data, validated
by configuration.py and applied by runner.py. Values never appear in an error
message or a log line -- a masking failure that printed the value it choked on
would leak exactly what it exists to hide.
"""
from __future__ import annotations

import datetime
import decimal
import hashlib
import hmac
import importlib
import json
import math
import random
import uuid
from typing import Any, Callable, ClassVar, Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple, Type

KEY_MINIMUM_LENGTH = 16

# Feistel rounds for the `key` strategy's permutation. FF1 and FF3-1 use 10 and
# 8; 10 is the conservative end, at a cost measured in microseconds per value.
FEISTEL_ROUNDS = 10


class MaskingError(Exception):
    """A policy that can't be applied: a column it doesn't cover, or a value of
    the wrong type for its strategy.

    Deterministic, so the runner never retries it. Messages name the column and
    the value's *type*, never the value.
    """


def _canonical(value: Any) -> bytes:
    """The bytes a value is keyed on.

    Numbers canonicalize to their plain decimal text, so an id read as an int
    from one database and as a Decimal or a whole float from another -- or as a
    varchar somewhere else -- masks identically. Dates canonicalize to ISO 8601, which is also how
    SQLite hands them back as text.
    """

    if isinstance(value, bool):
        return b'1' if value else b'0'
    if isinstance(value, int):
        return str(value).encode()
    if isinstance(value, decimal.Decimal):
        if value.is_finite() and value == value.to_integral_value():
            return str(int(value)).encode()
        return format(value.normalize(), 'f').encode()
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value)).encode()
        return repr(value).encode()
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat().encode()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)

    return str(value).encode('utf-8')


def keyFingerprint(key: str) -> str:
    """A short, non-reversible identifier for a key, safe to log and to record in
    a manifest. Two runs with the same fingerprint used the same key, so their
    masks agree; a changed fingerprint means every mask changed.
    """

    return hmac.digest(key.encode('utf-8'), b'lightweight-etl key fingerprint', 'sha256')[:6].hex()


class KeyedHash:
    """HMAC-SHA256 under a per-domain subkey.

    The subkey is derived once per column rather than mixing the domain into
    every message, which keeps the per-value cost to a single HMAC.
    """

    def __init__(self, key: str, domain: str) -> None:
        self._subkey = hmac.digest(key.encode('utf-8'), b'domain\x00' + domain.encode('utf-8'), 'sha256')


    def digest(self, message: bytes, purpose: bytes = b'') -> bytes:

        return hmac.digest(self._subkey, purpose + b'\x00' + message, 'sha256')


    def expand(self, message: bytes, length: int, purpose: bytes = b'') -> bytes:
        """`length` pseudorandom bytes, in counter mode past one digest's 32."""

        output = b''
        counter = 0

        while len(output) < length:
            output += self.digest(message, purpose + b'#' + counter.to_bytes(4, 'big'))
            counter += 1

        return output[:length]


    def below(self, message: bytes, upper: int, purpose: bytes = b'') -> int:
        """An integer in [0, upper). The modulo bias is below 2**-128 for any
        `upper` this package asks for, since 32 spare bytes are reduced.
        """

        width = max(32, (upper.bit_length() + 7) // 8 + 32)

        return int.from_bytes(self.expand(message, width, purpose), 'big') % upper


    def unit(self, message: bytes, purpose: bytes = b'') -> float:
        """A float in [0, 1) with 53 bits of resolution."""

        return (int.from_bytes(self.digest(message, purpose)[:7], 'big') >> 3) / float(1 << 53)


    def permute(self, size: int, value: int, purpose: bytes = b'') -> int:
        """A keyed permutation of range(size): every input maps to a distinct output.

        A balanced Feistel network over the smallest even bit width that covers
        `size`, with HMAC as the round function -- the same structure as NIST's
        FF1 and FF3-1, without their AES dependency. Inputs outside `size` are
        cycle-walked: the network is re-applied until the result lands back in
        range, which always terminates, because walking a permutation's cycle
        from an in-range point must return to one. The walk averages under four
        steps, since the bit width never exceeds four times `size`.
        """

        if size <= 1:
            return value

        bits = max(2, (size - 1).bit_length())
        bits += bits % 2
        half = bits // 2
        halfMask = (1 << half) - 1
        halfBytes = (half + 7) // 8
        prefix = purpose + b'|' + size.to_bytes((size.bit_length() + 7) // 8, 'big')

        result = value

        while True:
            left, right = result >> half, result & halfMask

            for roundNumber in range(FEISTEL_ROUNDS):
                roundValue = int.from_bytes(self.expand(right.to_bytes(halfBytes, 'big'), halfBytes, prefix + bytes([roundNumber])), 'big')
                left, right = right, left ^ (roundValue & halfMask)

            result = (left << half) | right

            if result < size:
                return result


class Strategy:
    """How one column is masked.

    Subclasses declare the options they accept in OPTIONS, as name -> the
    check applied to the raw configured value, and implement mask() for one
    non-NULL value. validateOptions runs at configuration time, so a bad option
    fails `lightweight-etl validate` rather than a job.

    NULL passes through untouched unless a strategy says otherwise: a NULL
    carries nothing to hide, and replacing it would change what a query like
    `where phone is null` returns.
    """

    NAME: ClassVar[str]
    OPTIONS: ClassVar[Dict[str, Callable[[Any], Any]]] = {}
    REQUIRED: ClassVar[Tuple[str, ...]] = ()
    # Whether the strategy consults the key at all. keep/null/constant don't,
    # and the manifest records that rather than implying a keyed transformation.
    KEYED: ClassVar[bool] = True

    def __init__(self, keyedHash: KeyedHash, options: Mapping[str, Any]) -> None:
        self.keyedHash = keyedHash
        self.options = dict(options)


    @classmethod
    def validateOptions(cls, options: Mapping[str, Any], label: Optional[str] = None) -> Dict[str, Any]:
        """`label` is how the policy named the strategy, for messages -- a
        custom strategy needn't set NAME.
        """

        label = label or getattr(cls, 'NAME', cls.__name__)

        unknown = sorted(set(options) - set(cls.OPTIONS))
        if unknown:
            allowed = ', '.join(sorted(cls.OPTIONS)) or 'none'
            raise ValueError('strategy "{}" does not take option(s) {} (it accepts: {})'.format(label, ', '.join(unknown), allowed))

        missing = [name for name in cls.REQUIRED if name not in options]
        if missing:
            raise ValueError('strategy "{}" requires option(s): {}'.format(label, ', '.join(missing)))

        validated = {}
        for name, value in options.items():
            try:
                validated[name] = cls.OPTIONS[name](value)
            except (TypeError, ValueError) as error:
                raise ValueError('strategy "{}" option {}: {}'.format(label, name, error)) from None

        cls.checkOptions(validated)

        return validated


    @classmethod
    def checkOptions(cls, options: Dict[str, Any]) -> None:
        """Cross-option rules, for the strategies that have any."""


    def maskColumn(self, values: Sequence[Any], chunkIndex: int) -> List[Any]:

        return [None if value is None else self.mask(value) for value in values]


    def mask(self, value: Any) -> Any:

        raise NotImplementedError


def _integerOption(minimum: int, maximum: Optional[int] = None) -> Callable[[Any], int]:

    def check(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError('must be a whole number')
        if value < minimum or (maximum is not None and value > maximum):
            raise ValueError('must be between {} and {}'.format(minimum, maximum) if maximum is not None else 'must be at least {}'.format(minimum))
        return value

    return check


def _numberOption(value: Any) -> decimal.Decimal:

    if isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal, str)):
        raise ValueError('must be a number')
    try:
        number = decimal.Decimal(str(value))
    except decimal.InvalidOperation:
        raise ValueError('must be a number') from None
    if not number.is_finite():
        raise ValueError('must be finite')

    return number


def _textOption(value: Any) -> str:

    if not isinstance(value, str) or not value:
        raise ValueError('must be non-empty text')

    return value


def _booleanOption(value: Any) -> bool:

    if not isinstance(value, bool):
        raise ValueError('must be true or false')

    return value


def _choiceOption(*choices: str) -> Callable[[Any], str]:

    def check(value: Any) -> str:
        if value not in choices:
            raise ValueError('must be one of: {}'.format(', '.join(choices)))
        return str(value)

    return check


def _typeName(value: Any) -> str:

    return type(value).__name__


class KeepStrategy(Strategy):
    """Leave the column as it is. The explicit way to say a column was reviewed."""

    NAME = 'keep'
    KEYED = False

    def maskColumn(self, values: Sequence[Any], chunkIndex: int) -> List[Any]:

        return list(values)


class NullStrategy(Strategy):
    """Replace every value with NULL. The right choice for free text, which can
    hold PII anywhere in it and has nothing a hash would usefully preserve.
    """

    NAME = 'null'
    KEYED = False

    def maskColumn(self, values: Sequence[Any], chunkIndex: int) -> List[Any]:

        return [None] * len(values)


class ConstantStrategy(Strategy):
    """Replace every value, NULLs included, with `value`."""

    NAME = 'constant'
    KEYED = False
    OPTIONS = {'value': lambda value: value}
    REQUIRED = ('value',)

    def maskColumn(self, values: Sequence[Any], chunkIndex: int) -> List[Any]:

        return [self.options['value']] * len(values)


class HashStrategy(Strategy):
    """An opaque hex token: `prefix` followed by `length` hex characters.

    The minimum length exists for unique columns. Twelve hex characters is 48
    bits, where a collision stays unlikely into the millions of distinct values;
    fewer bits would start breaking unique constraints at table sizes people
    actually have.
    """

    NAME = 'hash'
    OPTIONS = {'length': _integerOption(12, 64), 'prefix': lambda value: '' if value is None else str(value)}

    def mask(self, value: Any) -> Any:

        length = self.options.get('length', 16)

        return self.options.get('prefix', '') + self.keyedHash.digest(_canonical(value)).hex()[:length]


class EmailStrategy(Strategy):
    """Still shaped like an email address: `u<hex>@example.test`.

    Keyed on the lower-cased address, since email matching is effectively case
    insensitive and `Ann@X.com` and `ann@x.com` are the same person. The domain
    is replaced too, unless keepDomain is set -- a small company's domain can be
    as identifying as the name in front of it. `example.test` is reserved, so a
    masked address can never deliver mail to anyone.
    """

    NAME = 'email'
    OPTIONS = {'length': _integerOption(8, 40), 'mailDomain': _textOption, 'keepDomain': _booleanOption}

    @classmethod
    def checkOptions(cls, options: Dict[str, Any]) -> None:

        if options.get('keepDomain') and 'mailDomain' in options:
            raise ValueError('strategy "email" takes mailDomain or keepDomain, not both')


    def mask(self, value: Any) -> Any:

        if not isinstance(value, str):
            raise MaskingError('the email strategy needs text, got {}'.format(_typeName(value)))

        address = value.strip()
        local = 'u' + self.keyedHash.digest(address.lower().encode('utf-8')).hex()[:self.options.get('length', 12)]

        if self.options.get('keepDomain') and '@' in address:
            return local + '@' + address.rsplit('@', 1)[1]

        return local + '@' + self.options.get('mailDomain', 'example.test')


class DigitsStrategy(Strategy):
    """Replace every digit with a keyed digit, keeping everything else.

    `+1 (555) 010-9999` stays the same length with its punctuation in place, so
    formatting and length checks still pass. The key is the digits alone, so the
    same number masks the same way however it was formatted. keepLeading and
    keepTrailing preserve a country code or the last four of a card.

    An integer keeps its digit count and sign.
    """

    NAME = 'digits'
    OPTIONS = {'keepLeading': _integerOption(0), 'keepTrailing': _integerOption(0)}

    def _maskDigits(self, digits: str) -> str:

        keepLeading = self.options.get('keepLeading', 0)
        keepTrailing = self.options.get('keepTrailing', 0)
        stream = self.keyedHash.expand(digits.encode('ascii'), 2 * len(digits) + 32)
        generated = [str(byte % 10) for byte in stream if byte < 250]

        masked = []
        for position, digit in enumerate(digits):
            if position < keepLeading or position >= len(digits) - keepTrailing:
                masked.append(digit)
            else:
                masked.append(generated[position % len(generated)])

        return ''.join(masked)


    def mask(self, value: Any) -> Any:

        if isinstance(value, bool):
            raise MaskingError('the digits strategy needs text or an integer, got bool')

        if isinstance(value, int):
            text = str(abs(value))
            masked = self._maskDigits(text)
            if len(masked) > 1 and masked[0] == '0':
                # A leading zero would shorten the integer; keep its digit count.
                masked = str(int(self.keyedHash.below(text.encode('ascii'), 9, b'lead')) + 1) + masked[1:]
            return int(masked) * (-1 if value < 0 else 1)

        if not isinstance(value, str):
            raise MaskingError('the digits strategy needs text or an integer, got {}'.format(_typeName(value)))

        digits = ''.join(character for character in value if '0' <= character <= '9')
        if not digits:
            return value

        replacement = iter(self._maskDigits(digits))

        return ''.join(next(replacement) if '0' <= character <= '9' else character for character in value)


class NumberStrategy(Strategy):
    """A keyed number of the same type and precision.

    Either within a fixed range (`min` and `max`), or within `variance` of the
    original -- 0.1 by default, so 200.00 becomes something in [180.00, 220.00].
    Variance keeps magnitudes realistic, and so does leak them roughly; use a
    range when the magnitude itself is sensitive.

    The type is preserved: an int stays an int, a float a float, and a Decimal
    -- how PostgreSQL, MySQL and SQL Server return NUMERIC -- keeps its own
    number of decimal places. `decimals` overrides the places, and is worth
    setting for floats, which is how oracledb returns a NUMBER with a scale.
    """

    NAME = 'number'
    OPTIONS = {'min': _numberOption, 'max': _numberOption, 'variance': _numberOption, 'decimals': _integerOption(0, 38)}

    @classmethod
    def checkOptions(cls, options: Dict[str, Any]) -> None:

        hasMinimum, hasMaximum = 'min' in options, 'max' in options

        if hasMinimum != hasMaximum:
            raise ValueError('strategy "number" needs both min and max, or neither')
        if hasMinimum and options['min'] >= options['max']:
            raise ValueError('strategy "number" needs min below max')
        if hasMinimum and 'variance' in options:
            raise ValueError('strategy "number" takes a min/max range or a variance, not both')
        if 'variance' in options and not 0 < options['variance'] <= 1:
            raise ValueError('strategy "number" variance must be above 0 and at most 1')


    def _target(self, value: decimal.Decimal, message: bytes) -> decimal.Decimal:
        """The masked value before it's rounded to the column's precision."""

        fraction = decimal.Decimal(self.keyedHash.unit(message))

        if 'min' in self.options:
            return self.options['min'] + (self.options['max'] - self.options['min']) * fraction

        variance = self.options.get('variance', decimal.Decimal('0.1'))

        return value * (1 + (2 * fraction - 1) * variance)


    def _clamp(self, number: decimal.Decimal, step: decimal.Decimal) -> decimal.Decimal:
        """Rounding can step just outside a range whose bounds aren't on the grid."""

        if 'min' not in self.options:
            return number

        low = self.options['min'].quantize(step, rounding=decimal.ROUND_CEILING)
        high = self.options['max'].quantize(step, rounding=decimal.ROUND_FLOOR)

        return min(max(number, low), high)


    def mask(self, value: Any) -> Any:

        if isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal)):
            raise MaskingError('the number strategy needs a number, got {}'.format(_typeName(value)))

        message = _canonical(value)

        with decimal.localcontext() as context:
            context.prec = 60

            if isinstance(value, int):
                step = decimal.Decimal(1)
                masked = self._clamp(self._target(decimal.Decimal(value), message).quantize(step, rounding=decimal.ROUND_HALF_EVEN), step)
                return int(masked)

            if isinstance(value, float):
                if not math.isfinite(value):
                    return value
                target = self._target(decimal.Decimal(repr(value)), message)
                if 'decimals' in self.options:
                    step = decimal.Decimal(1).scaleb(-self.options['decimals'])
                    return float(self._clamp(target.quantize(step, rounding=decimal.ROUND_HALF_EVEN), step))
                return float(target)

            if not value.is_finite():
                return value

            exponent = -self.options['decimals'] if 'decimals' in self.options else min(0, int(value.as_tuple().exponent))
            step = decimal.Decimal(1).scaleb(exponent)

            return self._clamp(self._target(value, message).quantize(step, rounding=decimal.ROUND_HALF_EVEN), step)


class DateShiftStrategy(Strategy):
    """Move a date or timestamp by a keyed number of whole days, never zero.

    Whole days, so a timestamp keeps its time of day. Keyed on the value, so
    everyone born on the same day still shares a birthday after masking.
    ISO 8601 text -- how SQLite stores dates -- is parsed and written back in
    the same shape.
    """

    NAME = 'dateShift'
    OPTIONS = {'maxDays': _integerOption(1, 36500)}

    def _offset(self, value: Any) -> datetime.timedelta:

        maxDays = self.options.get('maxDays', 30)
        days = self.keyedHash.below(_canonical(value), 2 * maxDays) - maxDays

        return datetime.timedelta(days=days + 1 if days >= 0 else days)


    def mask(self, value: Any) -> Any:

        if isinstance(value, datetime.date):
            return value + self._offset(value)

        if not isinstance(value, str):
            raise MaskingError('the dateShift strategy needs a date, a timestamp or ISO 8601 text, got {}'.format(_typeName(value)))

        text = value.strip()

        try:
            if len(text) == 10:
                parsed: datetime.date = datetime.date.fromisoformat(text)
                return (parsed + self._offset(parsed)).isoformat()

            parsedTimestamp = datetime.datetime.fromisoformat(text)
        except ValueError:
            raise MaskingError('the dateShift strategy could not read a text value as an ISO 8601 date') from None

        shifted = parsedTimestamp + self._offset(parsedTimestamp)
        separator = 'T' if 'T' in text else ' '
        timespec = 'microseconds' if '.' in text else ('seconds' if text.count(':') >= 2 else 'minutes')

        return shifted.isoformat(sep=separator, timespec=timespec)


FIRST_NAMES = (
    'Ada', 'Alan', 'Alice', 'Amara', 'Andre', 'Anya', 'Arjun', 'Beatriz', 'Bruno', 'Camila', 'Carlos', 'Chen', 'Chloe', 'Dara', 'David',
    'Diego', 'Elena', 'Elif', 'Emeka', 'Emma', 'Ethan', 'Fatima', 'Felix', 'Freya', 'Grace', 'Hana', 'Hugo', 'Ines', 'Isaac', 'Ivan',
    'Jada', 'James', 'Jin', 'Jonas', 'Kai', 'Kofi', 'Lara', 'Leo', 'Lina', 'Lucas', 'Maya', 'Mateo', 'Mei', 'Mila', 'Nadia', 'Noah',
    'Nora', 'Omar', 'Oscar', 'Priya', 'Quinn', 'Rafael', 'Rosa', 'Sami', 'Sara', 'Theo', 'Uma', 'Victor', 'Wren', 'Yara', 'Yusuf', 'Zoe',
    )

LAST_NAMES = (
    'Abara', 'Alvarez', 'Anand', 'Bauer', 'Becker', 'Bianchi', 'Brooks', 'Castillo', 'Chandra', 'Costa', 'Dahl', 'Diallo', 'Dubois',
    'Eriksen', 'Ferreira', 'Fischer', 'Garcia', 'Haddad', 'Hansen', 'Hayes', 'Ibrahim', 'Ito', 'Jensen', 'Kaplan', 'Kim', 'Kowalski',
    'Larsen', 'Lopes', 'Mendes', 'Moreau', 'Murphy', 'Nakamura', 'Novak', 'Okafor', 'Olsen', 'Park', 'Patel', 'Perez', 'Quinlan',
    'Reyes', 'Rossi', 'Santos', 'Schmidt', 'Silva', 'Singh', 'Sousa', 'Tanaka', 'Torres', 'Vargas', 'Varga', 'Wagner', 'Walsh',
    'Weber', 'Wong', 'Yamada', 'Young', 'Zhang', 'Ziegler', 'Adeyemi', 'Bergstrom', 'Carvalho', 'Duarte', 'Falk', 'Lindqvist',
    )

CITIES = (
    'Ashford', 'Bayview', 'Brookfield', 'Cedar Falls', 'Clearwater', 'Crestwood', 'Eastport', 'Elmstead', 'Fairhaven', 'Glenmoor',
    'Greystone', 'Hartwell', 'Highbridge', 'Kingsley', 'Lakemont', 'Maple Grove', 'Marlow', 'Millbrook', 'Northgate', 'Oakridge',
    'Pinecrest', 'Port Albany', 'Ravenswood', 'Redcliff', 'Riverton', 'Rosedale', 'Sandhurst', 'Silverton', 'Southwick', 'Stonebridge',
    'Thornbury', 'Westbrook',
    )

COMPANY_WORDS = (
    'Acorn', 'Apex', 'Beacon', 'Blue Harbor', 'Brightline', 'Cobalt', 'Copperleaf', 'Evergreen', 'Fieldstone', 'Granite', 'Harborview',
    'Ironwood', 'Juniper', 'Keystone', 'Lighthouse', 'Meridian', 'Northwind', 'Oakline', 'Pioneer', 'Quarry', 'Redwood', 'Summit',
    'Tidewater', 'Vantage',
    )

COMPANY_SUFFIXES = ('Analytics', 'Group', 'Holdings', 'Industries', 'Labs', 'Logistics', 'Partners', 'Systems')

STREET_NAMES = (
    'Ash', 'Birch', 'Bridge', 'Canal', 'Cedar', 'Chapel', 'Church', 'Elm', 'Forest', 'Garden', 'Hill', 'Lake', 'Maple', 'Meadow',
    'Mill', 'Oak', 'Orchard', 'Park', 'Pine', 'River', 'School', 'Spring', 'Station', 'Willow',
    )

STREET_SUFFIXES = ('Street', 'Avenue', 'Road', 'Lane', 'Way', 'Drive', 'Court', 'Place')


class Locale(NamedTuple):
    """Names, places and address layout for one country's fake data.

    `address` is a format taking `number`, `street` (from `streets`) and `kind`
    (from `streetKinds`) -- the part that varies most between countries.
    """

    firstNames: Tuple[str, ...]
    lastNames: Tuple[str, ...]
    cities: Tuple[str, ...]
    streets: Tuple[str, ...]
    streetKinds: Tuple[str, ...]
    address: str
    companySuffixes: Tuple[str, ...]


def _words(text: str) -> Tuple[str, ...]:

    return tuple(word.strip() for word in text.split(',') if word.strip())


LOCALES: Dict[str, Locale] = {
    'en_US': Locale(
        _words('James, Mary, Robert, Patricia, John, Jennifer, Michael, Linda, David, Elizabeth, William, Barbara, Richard, Susan, '
               'Joseph, Jessica, Thomas, Sarah, Charles, Karen, Christopher, Lisa, Daniel, Nancy, Matthew, Betty, Anthony, Sandra'),
        _words('Smith, Johnson, Williams, Brown, Jones, Garcia, Miller, Davis, Rodriguez, Martinez, Hernandez, Lopez, Gonzalez, '
               'Wilson, Anderson, Thomas, Taylor, Moore, Jackson, Martin, Lee, Perez, Thompson, White, Harris, Sanchez, Clark, Lewis'),
        _words('Springfield, Riverside, Franklin, Greenville, Clinton, Fairview, Salem, Madison, Georgetown, Arlington, Ashland, '
               'Burlington, Manchester, Oxford, Milton, Clayton, Dayton, Lexington, Milford, Bristol'),
        _words('Main, Oak, Pine, Maple, Cedar, Elm, Washington, Lake, Hill, Park, Walnut, Spring'),
        _words('Street, Avenue, Road, Drive, Lane, Court, Boulevard, Way'),
        '{number} {street} {kind}', _words('Inc., LLC, Corp., Co.')),
    'en_GB': Locale(
        _words('Oliver, Amelia, George, Isla, Harry, Ava, Jack, Mia, Jacob, Emily, Charlie, Sophie, Thomas, Grace, Oscar, Lily, '
               'William, Freya, James, Evie, Alfie, Ella, Henry, Poppy'),
        _words('Smith, Jones, Taylor, Brown, Williams, Wilson, Johnson, Davies, Robinson, Wright, Thompson, Evans, Walker, White, '
               'Roberts, Green, Hall, Wood, Jackson, Clarke, Hughes, Edwards, Turner, Hill'),
        _words('Bradford, Chester, Durham, Exeter, Harrogate, Kendal, Lincoln, Ludlow, Norwich, Reading, Salisbury, Stafford, Truro, '
               'Wells, Whitby, Winchester, Worcester, York, Bath, Carlisle'),
        _words('High, Church, Station, Victoria, Park, Mill, Queen, King, School, London, Manor, Chapel'),
        _words('Street, Road, Lane, Close, Avenue, Way, Gardens, Crescent'),
        '{number} {street} {kind}', _words('Ltd, PLC, LLP')),
    'de_DE': Locale(
        _words('Lukas, Anna, Leon, Mia, Finn, Emma, Jonas, Hannah, Paul, Lea, Felix, Lena, Maximilian, Marie, Elias, Sophie, Noah, '
               'Laura, Ben, Julia, Tim, Lisa, Jan, Katharina'),
        _words('Müller, Schmidt, Schneider, Fischer, Weber, Meyer, Wagner, Becker, Schulz, Hoffmann, Schäfer, Koch, Bauer, Richter, '
               'Klein, Wolf, Schröder, Neumann, Schwarz, Zimmermann, Braun, Krüger, Hofmann, Hartmann'),
        _words('Aachen, Bamberg, Bielefeld, Bonn, Celle, Darmstadt, Erfurt, Freiburg, Göttingen, Heidelberg, Kassel, Kiel, Konstanz, '
               'Lübeck, Mainz, Münster, Passau, Regensburg, Trier, Ulm'),
        _words('Haupt, Bahnhof, Garten, Schul, Kirch, Linden, Berg, Wald, Mühlen, Dorf, Birken, Rosen'),
        _words('straße, weg, gasse, allee, ring, platz'),
        '{street}{kind} {number}', _words('GmbH, AG, KG, GmbH & Co. KG')),
    'fr_FR': Locale(
        _words('Gabriel, Emma, Léo, Jade, Raphaël, Louise, Arthur, Alice, Louis, Chloé, Lucas, Lina, Adam, Rose, Jules, Léa, Hugo, '
               'Anna, Maël, Mila, Nathan, Julia, Paul, Inès'),
        _words('Martin, Bernard, Dubois, Thomas, Robert, Richard, Petit, Durand, Leroy, Moreau, Simon, Laurent, Lefebvre, Michel, '
               'Garcia, David, Bertrand, Roux, Vincent, Fournier, Morel, Girard, André, Mercier'),
        _words('Amiens, Angers, Annecy, Avignon, Besançon, Brest, Caen, Colmar, Dijon, Grenoble, Limoges, Metz, Nancy, Nîmes, '
               'Orléans, Pau, Poitiers, Reims, Rouen, Tours'),
        _words("de la Paix, des Lilas, Victor Hugo, de la Gare, du Moulin, des Écoles, de l'Église, Pasteur, Jean Jaurès, "
               'du Château, des Tilleuls, de la République'),
        _words('rue, avenue, boulevard, place, allée, chemin'),
        '{number} {kind} {street}', _words('SARL, SAS, SA, EURL')),
    'es_ES': Locale(
        _words('Hugo, Lucía, Martín, Sofía, Daniel, Martina, Pablo, María, Alejandro, Julia, Lucas, Paula, Álvaro, Valeria, Adrián, '
               'Emma, Mateo, Daniela, David, Carla, Diego, Alba, Javier, Noa'),
        _words('García, Rodríguez, González, Fernández, López, Martínez, Sánchez, Pérez, Gómez, Martín, Jiménez, Ruiz, Hernández, '
               'Díaz, Moreno, Muñoz, Álvarez, Romero, Alonso, Gutiérrez, Navarro, Torres, Domínguez, Vázquez'),
        _words('Albacete, Alicante, Badajoz, Burgos, Cáceres, Cádiz, Córdoba, Gijón, Girona, Granada, Huelva, León, Logroño, Lugo, '
               'Oviedo, Salamanca, Santander, Segovia, Toledo, Zamora'),
        _words('Mayor, Real, del Sol, de la Paz, Nueva, del Carmen, San Juan, de la Iglesia, del Mar, de Cervantes, Colón, de Goya'),
        _words('Calle, Avenida, Plaza, Paseo, Camino, Ronda'),
        '{kind} {street}, {number}', _words('S.L., S.A., S.L.U.')),
    'pt_BR': Locale(
        _words('Miguel, Helena, Arthur, Alice, Gael, Laura, Heitor, Maria, Theo, Valentina, Davi, Heloísa, Gabriel, Sophia, Bernardo, '
               'Manuela, Samuel, Júlia, João, Isabela, Pedro, Lívia, Lucas, Beatriz'),
        _words('Silva, Santos, Oliveira, Souza, Rodrigues, Ferreira, Alves, Pereira, Lima, Gomes, Costa, Ribeiro, Martins, Carvalho, '
               'Almeida, Lopes, Soares, Fernandes, Vieira, Barbosa, Rocha, Dias, Nascimento, Andrade'),
        _words('Aracaju, Belém, Blumenau, Campinas, Cuiabá, Curitiba, Florianópolis, Goiânia, Joinville, Londrina, Maceió, Manaus, '
               'Natal, Niterói, Olinda, Petrópolis, Santos, Sorocaba, Uberlândia, Vitória'),
        _words('das Flores, São João, Sete de Setembro, XV de Novembro, das Palmeiras, Santa Luzia, do Comércio, Brasil, da Paz, '
               'Dom Pedro II, das Acácias, Tiradentes'),
        _words('Rua, Avenida, Travessa, Praça, Alameda, Estrada'),
        '{kind} {street}, {number}', _words('Ltda., S.A., ME')),
    'it_IT': Locale(
        _words('Leonardo, Sofia, Francesco, Aurora, Tommaso, Giulia, Edoardo, Ginevra, Alessandro, Beatrice, Lorenzo, Alice, Mattia, '
               'Vittoria, Gabriele, Emma, Riccardo, Ludovica, Andrea, Matilde, Diego, Chiara, Nicolò, Anna'),
        _words('Rossi, Russo, Ferrari, Esposito, Bianchi, Romano, Colombo, Ricci, Marino, Greco, Bruno, Gallo, Conti, De Luca, '
               'Mancini, Costa, Giordano, Rizzo, Lombardi, Moretti, Barbieri, Fontana, Santoro, Mariani'),
        _words('Ancona, Arezzo, Bergamo, Bologna, Brescia, Cagliari, Como, Cremona, Ferrara, Lecce, Lucca, Mantova, Modena, Padova, '
               'Parma, Perugia, Pisa, Ravenna, Siena, Trento'),
        _words('Roma, Garibaldi, Mazzini, Dante, Verdi, Cavour, Marconi, dei Mille, della Libertà, Vittorio Emanuele, San Francesco, '
               'del Popolo'),
        _words('Via, Viale, Piazza, Corso, Vicolo, Largo'),
        '{kind} {street} {number}', _words('S.r.l., S.p.A., S.a.s., S.n.c.')),
    'nl_NL': Locale(
        _words('Noah, Emma, Luca, Julia, Sem, Mila, Lucas, Tess, Levi, Sophie, Finn, Zoë, Daan, Sara, Milan, Nora, Bram, Yara, Mees, '
               'Eva, Jesse, Liv, Thijs, Anna'),
        _words('de Jong, Jansen, de Vries, van den Berg, van Dijk, Bakker, Janssen, Visser, Smit, Meijer, de Boer, Mulder, de Groot, '
               'Bos, Vos, Peters, Hendriks, van Leeuwen, Dekker, Brouwer, de Wit, Dijkstra, Smits, de Graaf'),
        _words('Alkmaar, Amersfoort, Apeldoorn, Arnhem, Breda, Delft, Deventer, Dordrecht, Enschede, Gouda, Groningen, Haarlem, '
               'Leeuwarden, Leiden, Maastricht, Nijmegen, Tilburg, Utrecht, Zwolle, Zaandam'),
        _words('Kerk, Molen, School, Dorps, Linden, Beuken, Stations, Wilhelmina, Juliana, Nieuwe, Oranje, Eiken'),
        _words('straat, weg, laan, plein, singel, gracht'),
        '{street}{kind} {number}', _words('B.V., N.V., V.O.F.')),
    }

# The lists used without a `locale` option: a deliberately international mix.
# Kept exactly as they were, since changing them would change every mask
# already written with them.
DEFAULT_LOCALE = Locale(FIRST_NAMES, LAST_NAMES, CITIES, STREET_NAMES, STREET_SUFFIXES, '{number} {street} {kind}', COMPANY_SUFFIXES)


class _FakeStrategy(Strategy):
    """A realistic-looking replacement, chosen from bundled lists by the hash.

    Not unique: the lists are small, so many values share a replacement. Use
    `hash` or `key` where uniqueness matters. maxLength truncates for narrow
    columns. `locale` picks a country's names, places and address layout;
    without it, the lists are an international mix.
    """

    OPTIONS = {'maxLength': _integerOption(1), 'locale': _choiceOption(*sorted(LOCALES))}

    @property
    def locale(self) -> Locale:

        return LOCALES[self.options['locale']] if 'locale' in self.options else DEFAULT_LOCALE

    def _pick(self, choices: Sequence[str], message: bytes, purpose: bytes) -> str:

        return choices[self.keyedHash.below(message, len(choices), purpose)]


    def generate(self, message: bytes) -> str:

        raise NotImplementedError


    def mask(self, value: Any) -> Any:

        generated = self.generate(_canonical(value))
        maxLength = self.options.get('maxLength')

        return generated[:maxLength] if maxLength else generated


class FakeFirstNameStrategy(_FakeStrategy):

    NAME = 'fakeFirstName'

    def generate(self, message: bytes) -> str:

        return self._pick(self.locale.firstNames, message, b'first')


class FakeLastNameStrategy(_FakeStrategy):

    NAME = 'fakeLastName'

    def generate(self, message: bytes) -> str:

        return self._pick(self.locale.lastNames, message, b'last')


class FakeNameStrategy(_FakeStrategy):

    NAME = 'fakeName'

    def generate(self, message: bytes) -> str:

        return '{} {}'.format(self._pick(self.locale.firstNames, message, b'first'), self._pick(self.locale.lastNames, message, b'last'))


class FakeCityStrategy(_FakeStrategy):

    NAME = 'fakeCity'

    def generate(self, message: bytes) -> str:

        return self._pick(self.locale.cities, message, b'city')


class FakeCompanyStrategy(_FakeStrategy):

    NAME = 'fakeCompany'

    def generate(self, message: bytes) -> str:

        return '{} {}'.format(self._pick(COMPANY_WORDS, message, b'company'), self._pick(self.locale.companySuffixes, message, b'suffix'))


class FakeStreetAddressStrategy(_FakeStrategy):

    NAME = 'fakeStreetAddress'

    def generate(self, message: bytes) -> str:

        number = self.keyedHash.below(message, 9999, b'number') + 1
        locale = self.locale

        return locale.address.format(number=number, street=self._pick(locale.streets, message, b'street'),
                                     kind=self._pick(locale.streetKinds, message, b'suffix'))


_ALPHANUMERIC_CLASSES = ('0123456789', 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
_HEX_DIGITS = '0123456789abcdef'


class KeyStrategy(Strategy):
    """A one-to-one mapping, safe for primary and foreign keys.

    Every distinct input gets a distinct output -- guaranteed by construction,
    not by probability -- so a masked primary key never collides, and a foreign
    key masked in the same domain still points at its row.

    The output has the input's shape:

    - An integer keeps its sign and digit count.
    - Text keeps its length and every character that isn't masked. `charset`
      decides which are: `alphanumeric` (the default) maps digits to digits and
      letters to letters of the same case; `digits` maps only digits; `hex`
      maps 0-9 and a-f, case-insensitively, for UUIDs and hex tokens.
    - A uuid.UUID stays a UUID. Its version digit isn't preserved.

    Shape preservation is what makes the mapping one-to-one overall: two inputs
    of different shapes can never produce the same output, and within a shape
    it is a permutation. That is also why `charset` is a fixed choice per column
    rather than detected per value -- detection would let two shapes overlap.
    """

    NAME = 'key'
    OPTIONS = {'charset': _choiceOption('alphanumeric', 'digits', 'hex')}

    def _maskInteger(self, value: int) -> int:

        magnitude = abs(value)
        digitCount = len(str(magnitude))
        # Zero belongs to the non-negative one-digit range only; letting a
        # negative digit map to it would make -0 collide with 0's own image.
        low = 10 ** (digitCount - 1) if digitCount > 1 or value < 0 else 0
        size = 10 ** digitCount - low
        masked = low + self.keyedHash.permute(size, magnitude - low, b'negative' if value < 0 else b'integer')

        return -masked if value < 0 else masked


    def _alphabets(self, text: str, charset: str) -> List[Optional[str]]:
        """Per position, the alphabet it's masked within, or None to keep it."""

        alphabets: List[Optional[str]] = []

        for character in text:
            if charset == 'hex':
                alphabets.append(_HEX_DIGITS if character.lower() in _HEX_DIGITS else None)
            elif charset == 'digits':
                alphabets.append(_ALPHANUMERIC_CLASSES[0] if '0' <= character <= '9' else None)
            else:
                alphabets.append(next((alphabet for alphabet in _ALPHANUMERIC_CLASSES if character in alphabet), None))

        return alphabets


    def _maskText(self, text: str, charset: str) -> str:

        alphabets = self._alphabets(text, charset)
        lowered = text.lower() if charset == 'hex' else text

        size = 1
        number = 0
        for character, alphabet in zip(lowered, alphabets):
            if alphabet is not None:
                size *= len(alphabet)
                number = number * len(alphabet) + alphabet.index(character)

        if size == 1:
            return text

        shape = ''.join(character if alphabet is None else str(len(alphabet)) for character, alphabet in zip(text, alphabets))
        masked = self.keyedHash.permute(size, number, b'text|' + charset.encode('ascii') + b'|' + shape.encode('utf-8'))

        characters = []
        for character, alphabet in reversed(list(zip(text, alphabets))):
            if alphabet is None:
                characters.append(character)
            else:
                masked, index = divmod(masked, len(alphabet))
                characters.append(alphabet[index])

        result = ''.join(reversed(characters))

        if charset == 'hex' and any(character in 'ABCDEF' for character in text) and not any(character in 'abcdef' for character in text):
            return result.upper()

        return result


    def mask(self, value: Any) -> Any:

        if isinstance(value, bool):
            raise MaskingError('the key strategy cannot mask a bool')

        if isinstance(value, int):
            return self._maskInteger(value)

        if isinstance(value, decimal.Decimal):
            if not (value.is_finite() and value == value.to_integral_value()):
                raise MaskingError('the key strategy needs a whole number, got a fractional Decimal')
            return decimal.Decimal(self._maskInteger(int(value)))

        if isinstance(value, uuid.UUID):
            return uuid.UUID(self._maskText(str(value), 'hex'))

        if isinstance(value, str):
            return self._maskText(value, self.options.get('charset', 'alphanumeric'))

        raise MaskingError('the key strategy needs an integer or text, got {}'.format(_typeName(value)))


_FPE_ALPHABETS = {
    'digits': '0123456789',
    'hex': '0123456789abcdef',
    'alphanumeric': '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ',
    }


class FPEStrategy(Strategy):
    """NIST FF1 format-preserving encryption (SP 800-38G Rev. 1), for policies
    that must name a published algorithm.

    One-to-one like `key`, and shaped like it: an integer keeps its sign and
    digit count; text keeps its length and every character outside `charset`.
    `charset` is `alphanumeric` (the default: any of 0-9, a-z and A-Z may
    become any other, so letters and digits can trade places), `digits`, or
    `hex` (case-insensitive, for UUIDs). The domain goes into FF1's tweak, and
    the AES-256 key is derived from the masking key.

    FF1 is only defined for at least a million possible values: six digits,
    five hex characters or four alphanumerics. Shorter values are masked with
    `key`'s permutation instead, which the manifest can't distinguish. The two
    never collide, since neither changes a value's length.

    Needs the `cryptography` package (`pip install lightweight-etl[fpe]`).
    """

    NAME = 'fpe'
    OPTIONS = {'charset': _choiceOption(*_FPE_ALPHABETS)}

    def __init__(self, keyedHash: KeyedHash, options: Mapping[str, Any]) -> None:
        super().__init__(keyedHash, options)
        self._ciphers: Dict[int, Any] = {}
        self._short = KeyStrategy(keyedHash, {})


    def _cipher(self, radix: int) -> Any:

        from .fpe import FF1

        if radix not in self._ciphers:
            self._ciphers[radix] = FF1(self.keyedHash.digest(b'', b'ff1 key'), radix)

        return self._ciphers[radix]


    def _maskInteger(self, value: int) -> int:

        digits = [int(character) for character in str(abs(value))]
        cipher = self._cipher(10)

        if len(digits) < cipher.minimumLength:
            return self._short._maskInteger(value)

        # Cycle-walk past results with a leading zero, which would shorten the
        # number. The input has none, so the walk comes back to such a value.
        tweak = b'negative' if value < 0 else b'integer'
        masked = cipher.encrypt(digits, tweak)
        while masked[0] == 0:
            masked = cipher.encrypt(masked, tweak)

        number = int(''.join(map(str, masked)))

        return -number if value < 0 else number


    def _maskText(self, text: str, charset: str) -> str:

        alphabet = _FPE_ALPHABETS[charset]
        lowered = text.lower() if charset == 'hex' else text
        positions = [index for index, character in enumerate(lowered) if character in alphabet]
        cipher = self._cipher(len(alphabet))

        if len(positions) < cipher.minimumLength:
            return self._short._maskText(text, charset)

        masked = set(positions)
        shape = ''.join('\x00' if index in masked else character for index, character in enumerate(text))
        numerals = cipher.encrypt([alphabet.index(lowered[index]) for index in positions], ('text|' + charset + '|' + shape).encode('utf-8'))

        characters = list(text)
        for index, numeral in zip(positions, numerals):
            characters[index] = alphabet[numeral]
        result = ''.join(characters)

        if charset == 'hex' and any(character in 'ABCDEF' for character in text) and not any(character in 'abcdef' for character in text):
            return result.upper()

        return result


    def mask(self, value: Any) -> Any:

        if isinstance(value, bool):
            raise MaskingError('the fpe strategy cannot mask a bool')

        if isinstance(value, int):
            return self._maskInteger(value)

        if isinstance(value, decimal.Decimal):
            if not (value.is_finite() and value == value.to_integral_value()):
                raise MaskingError('the fpe strategy needs a whole number, got a fractional Decimal')
            return decimal.Decimal(self._maskInteger(int(value)))

        if isinstance(value, uuid.UUID):
            return uuid.UUID(self._maskText(str(value), 'hex'))

        if isinstance(value, str):
            return self._maskText(value, self.options.get('charset', 'alphanumeric'))

        raise MaskingError('the fpe strategy needs an integer or text, got {}'.format(_typeName(value)))


class ShuffleStrategy(Strategy):
    """Shuffle the column's values among the rows of each chunk.

    Keeps the column's exact distribution, which is the whole point -- and why
    this is **not** anonymization: every real value is still in the table, on
    another row. Only rows in the same chunk are shuffled together, since a
    streamed job never holds more than one chunk.

    So a small chunk hides little: a chunk of one row -- the tail of a load, or
    an incremental run that found one changed row -- keeps its value on its own
    row, and any row keeps its own value with probability 1/len(chunk).
    """

    NAME = 'shuffle'

    def maskColumn(self, values: Sequence[Any], chunkIndex: int) -> List[Any]:

        shuffled = list(values)
        seed = int.from_bytes(self.keyedHash.digest(str(chunkIndex).encode('ascii'), b'shuffle'), 'big')
        random.Random(seed).shuffle(shuffled)

        return shuffled


STRATEGIES: Dict[str, Type[Strategy]] = {
    strategy.NAME: strategy for strategy in (
        KeepStrategy, NullStrategy, ConstantStrategy, HashStrategy, EmailStrategy, DigitsStrategy, NumberStrategy, DateShiftStrategy,
        FakeFirstNameStrategy, FakeLastNameStrategy, FakeNameStrategy, FakeCityStrategy, FakeCompanyStrategy, FakeStreetAddressStrategy,
        KeyStrategy, FPEStrategy, ShuffleStrategy,
        )
    }


def resolveStrategy(name: Any) -> Type[Strategy]:
    """A built-in strategy by name, or your own as `module.path:ClassName`.

    A custom strategy is a Strategy subclass: declare OPTIONS, and implement
    mask() for one non-NULL value, deriving it from self.keyedHash so it stays
    keyed and consistent. It must be importable wherever jobs run.
    """

    if isinstance(name, str) and name in STRATEGIES:
        return STRATEGIES[name]

    if not isinstance(name, str) or ':' not in name:
        raise ValueError('unknown strategy {!r}; choose from: {}, or name your own as module.path:ClassName'.format(
            name, ', '.join(sorted(STRATEGIES))))

    modulePath, _, attribute = name.partition(':')
    try:
        strategy = getattr(importlib.import_module(modulePath), attribute)
    except (ImportError, AttributeError) as error:
        raise ValueError('strategy {!r} could not be imported: {}'.format(name, error)) from None

    if not (isinstance(strategy, type) and issubclass(strategy, Strategy)):
        raise ValueError('strategy {!r} is not a subclass of lightweight_etl.masking.Strategy'.format(name))

    return strategy

# Fields of a column policy that belong to the policy itself rather than to its
# strategy. Everything else in the mapping is a strategy option.
POLICY_FIELDS = ('strategy', 'domain')


def validateColumnPolicy(policy: Any) -> Dict[str, Any]:
    """Normalizes one column's policy, raising ValueError if it's invalid.

    Accepts the shorthand `email: hash` as well as the mapping form, and returns
    the mapping form with the strategy's options checked and converted.
    """

    if isinstance(policy, str):
        policy = {'strategy': policy}

    if not isinstance(policy, Mapping):
        raise ValueError('a column policy is a strategy name or a mapping with a `strategy`')

    name = policy.get('strategy')
    strategy = resolveStrategy(name)

    domain = policy.get('domain')
    if domain is not None and (not isinstance(domain, str) or not domain):
        raise ValueError('domain must be non-empty text')

    options = {key: value for key, value in policy.items() if key not in POLICY_FIELDS}
    normalized: Dict[str, Any] = {'strategy': name}
    if domain is not None:
        normalized['domain'] = domain
    normalized.update(strategy.validateOptions(options, name))

    return normalized


def validateKey(key: str) -> None:

    if len(key) < KEY_MINIMUM_LENGTH:
        raise ValueError('the masking key must be at least {} characters; read it from the environment with ${{NAME}}'.format(KEY_MINIMUM_LENGTH))


class ColumnMasking(NamedTuple):
    """One column's resolved policy, as recorded in a manifest."""

    column: str
    strategy: str
    domain: Optional[str]
    source: str


class MaskingPlan:
    """A validated policy, bound to the columns a query actually returned.

    Binding is where fail-by-default happens. Every returned column must be
    covered, by name or by `defaultStrategy`, or bind() raises before a single
    row is written. The failure this guards against is the common one: a
    developer adds a column to production, and it flows into a non-production
    copy unmasked because nobody updated the policy. A column named in the
    policy but not returned is an error too, since it's almost always a typo
    that leaves the real column uncovered.

    Column names match case-insensitively, because Oracle reports unquoted
    identifiers in upper case whatever the policy says.

    The domain defaults to the column's own lower-cased name, so `email` in two
    tables already masks consistently. Set it explicitly to join differently
    named columns -- `orders.customerId` and `customers.id`.
    """

    def __init__(self, key: str, columns: Mapping[str, Mapping[str, Any]], defaultStrategy: Optional[Mapping[str, Any]] = None) -> None:
        validateKey(key)
        self.key = key
        self.columns = {name: validateColumnPolicy(policy) for name, policy in columns.items()}
        self.defaultStrategy = validateColumnPolicy(defaultStrategy) if defaultStrategy is not None else None

        folded: Dict[str, str] = {}
        for name in self.columns:
            if name.upper() in folded:
                raise ValueError('columns {!r} and {!r} differ only in case; column names match case-insensitively'.format(folded[name.upper()], name))
            folded[name.upper()] = name
        self._byFoldedName = folded


    @property
    def fingerprint(self) -> str:

        return keyFingerprint(self.key)


    def bind(self, columns: Sequence[str]) -> 'BoundMasking':

        returned = {column.upper() for column in columns}
        unknown = sorted(name for folded, name in self._byFoldedName.items() if folded not in returned)
        uncovered = [column for column in columns if column.upper() not in self._byFoldedName] if self.defaultStrategy is None else []

        problems = []
        if uncovered:
            problems.append('column(s) returned by sourceQuery but not in the masking policy: {}. '
                            'Add each one -- `keep` if it needs no masking -- or set defaultStrategy'.format(', '.join(uncovered)))
        if unknown:
            problems.append('column(s) in the masking policy that sourceQuery does not return: {}'.format(', '.join(unknown)))
        if problems:
            raise MaskingError('; '.join(problems))

        resolved = []
        for column in columns:
            name = self._byFoldedName.get(column.upper())
            policy = self.columns[name] if name is not None else self.defaultStrategy
            assert policy is not None
            resolved.append((column, policy, 'column' if name is not None else 'defaultStrategy'))

        return BoundMasking(self.key, resolved)


class BoundMasking:
    """Applies a bound plan to chunks of rows."""

    def __init__(self, key: str, resolved: Sequence[Tuple[str, Dict[str, Any], str]]) -> None:
        self.strategies: List[Strategy] = []
        self.manifest: List[ColumnMasking] = []

        for column, policy, source in resolved:
            strategyType = resolveStrategy(policy['strategy'])
            domain = policy.get('domain', column.lower())
            options = {name: value for name, value in policy.items() if name not in POLICY_FIELDS}
            self.strategies.append(strategyType(KeyedHash(key, domain), options))
            self.manifest.append(ColumnMasking(column=column, strategy=policy['strategy'], domain=domain if strategyType.KEYED else None, source=source))

        self._chunkIndex = 0
        self._passthrough = all(isinstance(strategy, KeepStrategy) for strategy in self.strategies)


    def apply(self, rows: Sequence[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:

        chunkIndex = self._chunkIndex
        self._chunkIndex += 1

        if not rows or self._passthrough:
            return list(rows)

        maskedColumns = []
        for index, (strategy, entry) in enumerate(zip(self.strategies, self.manifest)):
            try:
                maskedColumns.append(strategy.maskColumn([row[index] for row in rows], chunkIndex))
            except MaskingError as error:
                raise MaskingError('column "{}": {}'.format(entry.column, error)) from None

        return list(zip(*maskedColumns))


def buildMaskingManifest(outcomes: Sequence[Any], declared: Mapping[str, Mapping[str, Any]],
                         generatedAt: Optional[datetime.datetime] = None) -> Dict[str, Any]:
    """What was masked, how, and under which key -- the artifact an auditor asks for.

    `declared` maps each masked job to what its configuration says (target and
    key fingerprint). A completed job's outcome adds what actually happened: the
    strategy and domain applied to each column the query returned, and the row
    count. A masked job that failed or was skipped is still listed, with its
    status and no columns, because "this copy was not refreshed" is something
    an auditor needs to see too. Never a value, and never the key.
    """

    jobs = []
    byJob = {outcome.job: outcome for outcome in outcomes}

    for job, entry in declared.items():
        outcome = byJob.get(job)
        if outcome is None:
            continue
        applied = getattr(outcome, 'masking', None) or {}
        record: Dict[str, Any] = {'job': job, 'status': outcome.status.value}
        record.update(entry)
        record.update(rowCount=outcome.rowCount, columns=applied.get('columns', []))
        jobs.append(record)

    timestamp = generatedAt or datetime.datetime.now(datetime.timezone.utc)

    return {'generatedAt': timestamp.isoformat(timespec='seconds'), 'jobs': jobs}


INTEGRITY_FIELD = 'integrity'


class ManifestVerification(NamedTuple):
    """What verifyManifest found. `signed` and `signatureValid` are both False
    for an unsigned manifest, which proves only that it is intact.
    """

    digestValid: bool
    signed: bool
    signatureValid: bool
    signingKeyFingerprint: Optional[str]


def _canonicalManifest(manifest: Mapping[str, Any]) -> bytes:
    """The bytes a manifest's digest covers: every field but `integrity`, as
    sorted, compact JSON -- so whitespace and key order don't matter, and any
    change to a value does.
    """

    body = {name: value for name, value in manifest.items() if name != INTEGRITY_FIELD}

    return json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str).encode('utf-8')


def sealManifest(manifest: Mapping[str, Any], signingKey: Optional[str] = None) -> Dict[str, Any]:
    """The manifest with an `integrity` section: a SHA-256 digest of its
    content and, given a signing key, an HMAC-SHA256 signature.

    The digest shows a manifest hasn't been altered since it was written; only
    the signature shows who wrote it, since anyone can recompute a digest.
    The signing key should differ from any masking key -- the manifest records
    its fingerprint, so verifiers know which key to ask for.

    The manifest is passed through JSON first, so what is sealed is exactly
    what a verifier will read back from the file.
    """

    sealed: Dict[str, Any] = json.loads(json.dumps(dict(manifest), default=str))
    canonical = _canonicalManifest(sealed)
    integrity: Dict[str, Any] = {'algorithm': 'sha256', 'digest': hashlib.sha256(canonical).hexdigest()}

    if signingKey is not None:
        validateKey(signingKey)
        integrity.update(signatureAlgorithm='hmac-sha256', signature=hmac.new(signingKey.encode('utf-8'), canonical, 'sha256').hexdigest(),
                         signingKeyFingerprint=keyFingerprint(signingKey))

    sealed[INTEGRITY_FIELD] = integrity

    return sealed


def verifyManifest(manifest: Mapping[str, Any], signingKey: Optional[str] = None) -> ManifestVerification:
    """Checks a sealed manifest. A signed one is only checked against a key
    with the fingerprint it records; with no key, or another key, its
    signature counts as not valid.
    """

    integrity = manifest.get(INTEGRITY_FIELD)
    if not isinstance(integrity, Mapping) or integrity.get('algorithm') != 'sha256':
        raise ValueError('the manifest has no integrity section this version can check')

    canonical = _canonicalManifest(manifest)
    digestValid = hmac.compare_digest(hashlib.sha256(canonical).hexdigest(), str(integrity.get('digest', '')))
    signed = 'signature' in integrity
    fingerprint = integrity.get('signingKeyFingerprint')
    signatureValid = False

    if signed and signingKey is not None and keyFingerprint(signingKey) == fingerprint and integrity.get('signatureAlgorithm') == 'hmac-sha256':
        expected = hmac.new(signingKey.encode('utf-8'), canonical, 'sha256').hexdigest()
        signatureValid = hmac.compare_digest(expected, str(integrity['signature']))

    return ManifestVerification(digestValid=digestValid, signed=signed, signatureValid=signatureValid, signingKeyFingerprint=fingerprint)
