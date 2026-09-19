"""The masking strategies that ship with the package: `keep` through `redact`,
and the lists the `fake*` ones pick from.

The fake* strategies' lists are in fakeData. A policy names these by their NAME; your own strategies are referenced as
module.path:ClassName and subclass bauta.masking.Strategy, as these do, so
nothing here is privileged. masking.py holds what they share -- the keyed
hash, the Strategy base, the native masker -- and the plans that apply them.

Changing what any of these returns changes every mask already made with it:
mask-rs/vectors/reference.json records them, and the native masker must match.
"""
from __future__ import annotations

import datetime
import decimal
import math
import random
import re
import unicodedata
import uuid
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Type

from .fakeData import COMPANY_WORDS, DEFAULT_LOCALE, LOCALES, Locale
from .masking import MAXIMUM_KEY_LENGTH, KeyedHash, MaskingError, Strategy, _canonical


def _asciiDigits(text: str) -> str:
    """`text` with every decimal digit, in any script, written as 0-9."""

    if text.isascii():
        return text

    return ''.join(str(unicodedata.decimal(character)) if unicodedata.decimal(character, None) is not None else character for character in text)


def _inScriptOf(original: str, masked: str) -> str:
    """`masked`, which has 0-9 where `original` has a decimal digit, with each
    of those digits written back in the script of the digit it replaced.
    """

    if original.isascii():
        return masked

    characters = []
    for before, after in zip(original, masked):
        value = unicodedata.decimal(before, None)
        characters.append(chr(ord(before) - value + int(after)) if value is not None else after)

    return ''.join(characters)


def _digitCount(value: int) -> int:
    """How many digits an integer has, without writing it out -- which Python
    refuses past 4,300 digits."""

    magnitude = abs(value)
    if magnitude >= 10 ** MAXIMUM_KEY_LENGTH:
        return MAXIMUM_KEY_LENGTH + 1

    return len(str(magnitude))


def _requireIdentifierLength(strategy: str, length: int) -> None:

    if length > MAXIMUM_KEY_LENGTH:
        raise MaskingError('the {} strategy masks identifiers of up to {} characters or digits, and this value is longer; '
                           'use hash, redact or null for long values'.format(strategy, MAXIMUM_KEY_LENGTH))


def _requireAsciiCharset(strategy: str, text: str, charset: str) -> None:
    """Refuses text holding letters or digits the charset can't mask, which
    would otherwise be copied unmasked while the manifest says masked.
    """

    if text.isascii():
        return

    if charset == 'alphanumeric':
        if any(not character.isascii() and character.isalnum() for character in text):
            raise MaskingError('the {} strategy masks only ASCII letters and digits, and this value has letters or digits in another script, '
                               'which it would copy unmasked; use hash, a fake strategy or null for such text'.format(strategy))
    elif any(not character.isascii() and character.isdigit() for character in text):
        raise MaskingError('the {} strategy masks only the digits 0-9, and this value has digits in another script, '
                           'which it would copy unmasked; use the digits strategy for such text'.format(strategy))




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
    """Replace every value with NULL."""

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
    """An opaque hex token: `prefix` followed by `length` hex characters. At
    least 12 (48 bits), so unique columns stay unique into the millions.
    """

    NAME = 'hash'
    NATIVE = 'hash'
    CACHEABLE = True
    OPTIONS = {'length': _integerOption(12, 64), 'prefix': lambda value: '' if value is None else str(value)}

    def mask(self, value: Any) -> Any:

        length = self.options.get('length', 16)

        return self.options.get('prefix', '') + self.keyedHash.digest(_canonical(value)).hex()[:length]


class EmailStrategy(Strategy):
    """Still shaped like an email address: `u<hex>@example.test`, a reserved
    domain that can't deliver mail. Keyed on the lower-cased address.
    """

    NAME = 'email'
    NATIVE = 'email'
    CACHEABLE = True
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
    """Replace every digit with a keyed digit, keeping everything else. Keyed
    on the digits alone, so formatting doesn't change the mask.
    """

    NAME = 'digits'
    NATIVE = 'digits'
    CACHEABLE = True
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

        if any(not character.isascii() and character.isdigit() and unicodedata.decimal(character, None) is None for character in value):
            raise MaskingError('the digits strategy masks decimal digits, and this value has other digit characters '
                               '(superscript or circled, say), which it would copy unmasked')

        # Digits in any script are keyed as 0-9, so a number masks the same way
        # whichever digits it was written in, and are written back in their own.
        normalized = _asciiDigits(value)
        digits = ''.join(character for character in normalized if '0' <= character <= '9')
        if not digits:
            return value

        replacement = iter(self._maskDigits(digits))
        masked = ''.join(next(replacement) if '0' <= character <= '9' else character for character in normalized)

        return _inScriptOf(value, masked)


class NumberStrategy(Strategy):
    """A keyed number of the same type and precision, within `min`-`max` or
    within `variance` of the original. A value the variance would round back
    to itself moves one step instead; zero stays zero.

    `decimals` overrides the precision -- worth setting for floats, which is
    how oracledb returns a NUMBER with a scale.
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


    def _moved(self, value: decimal.Decimal, masked: decimal.Decimal, step: decimal.Decimal, message: bytes) -> decimal.Decimal:
        """`masked`, or one step from `value` if the variance rounded it back to
        `value`. The direction is keyed too.
        """

        if 'min' in self.options or masked != value or value == 0:
            return masked

        return value + step if self.keyedHash.unit(message, b'step') >= 0.5 else value - step


    def mask(self, value: Any) -> Any:

        if isinstance(value, bool) or not isinstance(value, (int, float, decimal.Decimal)):
            raise MaskingError('the number strategy needs a number, got {}'.format(_typeName(value)))

        message = _canonical(value)

        with decimal.localcontext() as context:
            context.prec = 60

            if isinstance(value, int):
                step = decimal.Decimal(1)
                masked = self._clamp(self._target(decimal.Decimal(value), message).quantize(step, rounding=decimal.ROUND_HALF_EVEN), step)
                return int(self._moved(decimal.Decimal(value), masked, step, message))

            if isinstance(value, float):
                if not math.isfinite(value):
                    return value
                target = self._target(decimal.Decimal(repr(value)), message)
                if 'decimals' in self.options:
                    step = decimal.Decimal(1).scaleb(-self.options['decimals'])
                    masked = self._clamp(target.quantize(step, rounding=decimal.ROUND_HALF_EVEN), step)
                    return float(self._moved(decimal.Decimal(repr(value)), masked, step, message))
                return float(target)

            if not value.is_finite():
                return value

            exponent = -self.options['decimals'] if 'decimals' in self.options else min(0, int(value.as_tuple().exponent))
            step = decimal.Decimal(1).scaleb(exponent)

            masked = self._clamp(self._target(value, message).quantize(step, rounding=decimal.ROUND_HALF_EVEN), step)

            return self._moved(value, masked, step, message)


_CALENDAR_ENDS = frozenset({datetime.date.min.toordinal(), datetime.date.max.toordinal()})
_CALENDAR_INSIDE = range(datetime.date.min.toordinal() + 1, datetime.date.max.toordinal())


class DateShiftStrategy(Strategy):
    """Move a date or timestamp by a keyed number of whole days, never zero.
    ISO 8601 text is written back in the same shape.

    0001-01-01 and 9999-12-31 mean "no date" or "forever", so they are kept,
    and a shift that would leave the calendar or land on them goes the other way.
    """

    NAME = 'dateShift'
    OPTIONS = {'maxDays': _integerOption(1, 36500)}

    def _offset(self, value: Any) -> datetime.timedelta:

        maxDays = self.options.get('maxDays', 30)
        days = self.keyedHash.below(_canonical(value), 2 * maxDays) - maxDays

        return datetime.timedelta(days=days + 1 if days >= 0 else days)


    def _shift(self, value: Any) -> Any:
        """`value`, a date or datetime, moved by its offset."""

        ordinal = value.toordinal()
        if ordinal in _CALENDAR_ENDS:
            return value

        offset = self._offset(value)
        if ordinal + offset.days not in _CALENDAR_INSIDE:
            offset = -offset

        return value + offset


    def mask(self, value: Any) -> Any:

        if isinstance(value, datetime.date):
            return self._shift(value)

        if not isinstance(value, str):
            raise MaskingError('the dateShift strategy needs a date, a timestamp or ISO 8601 text, got {}'.format(_typeName(value)))

        text = value.strip()

        try:
            if len(text) == 10:
                parsed: datetime.date = datetime.date.fromisoformat(text)
                return self._shift(parsed).isoformat()

            parsedTimestamp = datetime.datetime.fromisoformat(text)
        except ValueError:
            raise MaskingError('the dateShift strategy could not read a text value as an ISO 8601 date') from None

        shifted = self._shift(parsedTimestamp)
        separator = 'T' if 'T' in text else ' '
        timespec = 'microseconds' if '.' in text else ('seconds' if text.count(':') >= 2 else 'minutes')

        return shifted.isoformat(sep=separator, timespec=timespec)


class _FakeStrategy(Strategy):
    """A realistic-looking replacement, chosen from bundled lists by the hash.
    Not unique.
    """

    CACHEABLE = True
    OPTIONS = {'maxLength': _integerOption(1), 'locale': _choiceOption(*sorted(LOCALES))}

    @property
    def locale(self) -> Locale:

        return LOCALES[self.options['locale']] if 'locale' in self.options else DEFAULT_LOCALE


    def _nativeOptions(self) -> Dict[str, Any]:
        """The lists themselves, so they are defined once, here: the native
        masker picks from what it's handed.
        """

        locale = self.locale

        return {'maxLength': self.options.get('maxLength'), 'firstNames': list(locale.firstNames), 'lastNames': list(locale.lastNames),
                'cities': list(locale.cities), 'streets': list(locale.streets), 'streetKinds': list(locale.streetKinds),
                'address': locale.address, 'companySuffixes': list(locale.companySuffixes), 'companyWords': list(COMPANY_WORDS)}

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
    NATIVE = 'fakeFirstName'

    def generate(self, message: bytes) -> str:

        return self._pick(self.locale.firstNames, message, b'first')


class FakeLastNameStrategy(_FakeStrategy):

    NAME = 'fakeLastName'
    NATIVE = 'fakeLastName'

    def generate(self, message: bytes) -> str:

        return self._pick(self.locale.lastNames, message, b'last')


class FakeNameStrategy(_FakeStrategy):

    NAME = 'fakeName'
    NATIVE = 'fakeName'

    def generate(self, message: bytes) -> str:

        return '{} {}'.format(self._pick(self.locale.firstNames, message, b'first'), self._pick(self.locale.lastNames, message, b'last'))


class FakeCityStrategy(_FakeStrategy):

    NAME = 'fakeCity'
    NATIVE = 'fakeCity'

    def generate(self, message: bytes) -> str:

        return self._pick(self.locale.cities, message, b'city')


class FakeCompanyStrategy(_FakeStrategy):

    NAME = 'fakeCompany'
    NATIVE = 'fakeCompany'

    def generate(self, message: bytes) -> str:

        return '{} {}'.format(self._pick(COMPANY_WORDS, message, b'company'), self._pick(self.locale.companySuffixes, message, b'suffix'))


class FakeStreetAddressStrategy(_FakeStrategy):

    NAME = 'fakeStreetAddress'
    NATIVE = 'fakeStreetAddress'

    def generate(self, message: bytes) -> str:

        number = self.keyedHash.below(message, 9999, b'number') + 1
        locale = self.locale

        return locale.address.format(number=number, street=self._pick(locale.streets, message, b'street'),
                                     kind=self._pick(locale.streetKinds, message, b'suffix'))


_ALPHANUMERIC_CLASSES = ('0123456789', 'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')
_HEX_DIGITS = '0123456789abcdef'


class KeyStrategy(Strategy):
    """A one-to-one mapping, safe for primary and foreign keys, that keeps the
    input's shape: an integer's sign and digit count, text's length and every
    character outside `charset`.

    One-to-one because it is a permutation within each shape, and different
    shapes can't meet. That is why `charset` is fixed per column rather than
    detected per value, which would let shapes overlap.
    """

    NAME = 'key'
    NATIVE = 'key'
    CACHEABLE = True
    OPTIONS = {'charset': _choiceOption('alphanumeric', 'digits', 'hex')}

    def _maskInteger(self, value: int) -> int:

        _requireIdentifierLength('key', _digitCount(value))
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

        _requireIdentifierLength('key', len(text))
        _requireAsciiCharset('key', text, charset)
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
    """NIST FF1 format-preserving encryption (SP 800-38G Rev. 1), shaped like
    `key`. The domain goes into FF1's tweak.

    FF1 needs at least a million possible values, so shorter values fall back
    to `key`'s permutation -- no collision, since neither changes a length --
    or, with `strict`, fail. Needs the `fpe` extra.
    """

    NAME = 'fpe'
    NATIVE = 'fpe'
    CACHEABLE = True
    OPTIONS = {'charset': _choiceOption(*_FPE_ALPHABETS), 'strict': _booleanOption}

    def __init__(self, keyedHash: KeyedHash, options: Mapping[str, Any]) -> None:
        super().__init__(keyedHash, options)
        self._ciphers: Dict[int, Any] = {}
        self._short = KeyStrategy(keyedHash, {})


    def _cipher(self, radix: int) -> Any:

        from .fpe import FF1

        if radix not in self._ciphers:
            self._ciphers[radix] = FF1(self.keyedHash.digest(b'', b'ff1 key'), radix)

        return self._ciphers[radix]


    def _tooShort(self, cipher: Any, what: str) -> None:
        """Raises under `strict`; otherwise the caller falls back to `key`."""

        if self.options.get('strict'):
            raise MaskingError('the fpe strategy is strict, and FF1 needs at least {} {} in a value; this one has fewer'.format(
                cipher.minimumLength, what))


    def _maskInteger(self, value: int) -> int:

        _requireIdentifierLength('fpe', _digitCount(value))
        digits = [int(character) for character in str(abs(value))]
        cipher = self._cipher(10)

        if len(digits) < cipher.minimumLength:
            self._tooShort(cipher, 'digits')
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

        _requireIdentifierLength('fpe', len(text))
        _requireAsciiCharset('fpe', text, charset)
        alphabet = _FPE_ALPHABETS[charset]
        lowered = text.lower() if charset == 'hex' else text
        positions = [index for index, character in enumerate(lowered) if character in alphabet]
        cipher = self._cipher(len(alphabet))

        if len(positions) < cipher.minimumLength:
            self._tooShort(cipher, '{} characters'.format(charset))
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


def _listOption(*choices: str) -> Callable[[Any], List[str]]:

    def check(value: Any) -> List[str]:
        if not isinstance(value, list) or not value or any(item not in choices for item in value):
            raise ValueError('must be a non-empty list of: {}'.format(', '.join(choices)))
        return list(value)

    return check


def _patternsOption(value: Any) -> List[str]:

    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
        raise ValueError('must be a non-empty list of regular expressions')
    for pattern in value:
        try:
            re.compile(pattern)
        except re.error as error:
            raise ValueError('{!r} is not a valid regular expression: {}'.format(pattern, error)) from None

    return list(value)


def _luhn(digits: str) -> bool:

    total = 0
    for position, character in enumerate(reversed(digits)):
        digit = int(character)
        if position % 2:
            digit = digit * 2 - 9 if digit > 4 else digit * 2
        total += digit

    return total % 10 == 0


def _validIban(text: str) -> bool:

    compact = text.replace(' ', '')
    rearranged = compact[4:] + compact[:4]

    return 15 <= len(compact) <= 34 and int(''.join(str(int(character, 36)) for character in rearranged)) % 97 == 1


def _validIpv4(text: str) -> bool:

    return all(int(octet) <= 255 for octet in text.split('.'))


_DATE_LIKE = re.compile(r'^(?:\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})$')


def _phoneLike(text: str) -> bool:
    """7 to 15 digits -- E.164's limit -- and not a date, whose separators
    otherwise make it look like a number.
    """

    return 7 <= sum(character.isdigit() for character in text) <= 15 and not _DATE_LIKE.match(text.strip())


# (kind, pattern, check). Earlier kinds win where matches overlap, so the
# specific ones -- validated by a checksum or a fixed shape -- come before the
# loose phone pattern, which would otherwise swallow a card number.
_DETECTORS: Tuple[Tuple[str, 're.Pattern[str]', Callable[[str], bool]], ...] = (
    ('email', re.compile(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}'), lambda text: True),
    ('iban', re.compile(r'\b[A-Z]{2}\d{2}(?: ?[A-Z\d]){11,30}\b'), _validIban),
    ('card', re.compile(r'(?<![\d-])\d(?:[ -]?\d){12,18}(?![\d-])'), lambda text: _luhn(re.sub(r'\D', '', text))),
    ('ssn', re.compile(r'(?<![\d-])\d{3}-\d{2}-\d{4}(?![\d-])'), lambda text: True),
    ('ip', re.compile(r'(?<![\d.])(?:\d{1,3}\.){3}\d{1,3}(?!\.?\d)'), _validIpv4),
    ('phone', re.compile(r'(?<![\w+])\+?\(?\d[\d ().-]{5,}\d(?!\w)'), _phoneLike),
    )

_DETECTOR_KINDS = tuple(kind for kind, _, _ in _DETECTORS)


class RedactStrategy(Strategy):
    """Finds recognisable identifiers inside free text and replaces only those,
    with a label or a keyed value of the same shape. It cannot find names.
    """

    NAME = 'redact'
    OPTIONS = {'replacement': _choiceOption('label', 'mask'), 'detect': _listOption(*_DETECTOR_KINDS), 'patterns': _patternsOption}

    def __init__(self, keyedHash: KeyedHash, options: Mapping[str, Any]) -> None:
        super().__init__(keyedHash, options)
        kinds = set(self.options.get('detect', _DETECTOR_KINDS))
        self._detectors = [detector for detector in _DETECTORS if detector[0] in kinds]
        self._detectors += [('pattern', re.compile(pattern), lambda text: True) for pattern in self.options.get('patterns', [])]
        self._email = EmailStrategy(keyedHash, {})
        self._digits = DigitsStrategy(keyedHash, {})
        self._card = DigitsStrategy(keyedHash, {'keepTrailing': 4})
        self._key = KeyStrategy(keyedHash, {})


    def _spans(self, text: str) -> List[Tuple[int, int, str]]:
        """Non-overlapping (start, end, kind), earlier detectors winning."""

        taken: List[Tuple[int, int, str]] = []
        # One byte per character, set once a span claims it: checking a match
        # against it costs the match's length, not the number of spans so far.
        occupied = bytearray(len(text))
        for kind, pattern, check in self._detectors:
            for match in pattern.finditer(text):
                start, end = match.span()
                if start == end or occupied.find(1, start, end) != -1 or not check(match.group(0)):
                    continue
                occupied[start:end] = b'\x01' * (end - start)
                taken.append((start, end, kind))

        return sorted(taken)


    def _replace(self, kind: str, found: str) -> str:

        if self.options.get('replacement', 'label') == 'label':
            return '[REDACTED]' if kind == 'pattern' else '[{}]'.format(kind.upper())

        if kind == 'email':
            return str(self._email.mask(found))
        if kind == 'card':
            return str(self._card.mask(found))
        if kind in ('phone', 'ssn'):
            return str(self._digits.mask(found))
        if kind == 'iban':
            # The detectors accept digits in any script; the key strategy
            # doesn't, so they're masked as 0-9 and written back in their own.
            rest = found[2:]
            return found[:2] + _inScriptOf(rest, str(self._key.mask(_asciiDigits(rest))))
        if kind == 'ip':
            octets = self.keyedHash.digest(_asciiDigits(found).encode('ascii'), b'ip')
            return '10.{}.{}.{}'.format(octets[0], octets[1], octets[2])

        return 'redacted-' + self.keyedHash.digest(found.encode('utf-8'), b'pattern').hex()[:12]


    def mask(self, value: Any) -> Any:

        if not isinstance(value, str):
            raise MaskingError('the redact strategy needs text, got {}'.format(_typeName(value)))

        pieces = []
        position = 0
        for start, end, kind in self._spans(value):
            pieces.append(value[position:start])
            pieces.append(self._replace(kind, value[start:end]))
            position = end
        pieces.append(value[position:])

        return ''.join(pieces)


class ShuffleStrategy(Strategy):
    """Shuffle the column's values among the rows of each chunk. Not
    anonymization: every real value stays in the table, and a small chunk
    barely moves them.
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
        KeyStrategy, FPEStrategy, RedactStrategy, ShuffleStrategy,
        )
    }
