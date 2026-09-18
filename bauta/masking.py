"""Deterministic, keyed masking, applied to rows on their way to the target.

Every masked value is derived from a keyed hash of the value itself, scoped by a
named *domain*:

    mask = strategy( HMAC(key, domain, value) )

So the same value in the same domain masks the same way in every table and on
every run, and nobody without the key can match candidate values to masks.

Nothing here imports the rest of the package. Values never appear in an error
message or a log line.
"""
from __future__ import annotations

import datetime
import decimal
import hashlib
import functools
import hmac
import importlib
import importlib.metadata
import json
import logging
import os
import math
import random
import re
import unicodedata
import uuid
from typing import Any, Callable, ClassVar, Dict, List, Mapping, NamedTuple, Optional, Sequence, Tuple, Type, Union

from .log import LOGGER_NAME

KEY_MINIMUM_LENGTH = 16

# Masks remembered per column, for the repeated values of foreign keys and
# low-cardinality columns. Bounded to a few megabytes a column.
MASK_CACHE_SIZE = 16384
MASK_CACHE_MAXIMUM_TEXT = 256

# The types whose equal values always mask the same way. Equal Decimals,
# floats and datetimes can differ in scale, sign of zero or time zone, which
# some strategies keep, so they aren't cached; nor is bool, which equals 1.
_CACHEABLE_TYPES = frozenset({str, int, uuid.UUID})

# The longest value `key` and `fpe` mask, in characters or digits. Their cost
# grows with the square of the length.
MAXIMUM_KEY_LENGTH = 256

# Feistel rounds for `key`'s permutation: FF1's 10, the conservative end.
FEISTEL_ROUNDS = 10

# SHA-256's compression block, which is what HMAC pads its key out to.
HMAC_BLOCK_SIZE = 64


@functools.cache
def _nativeModule() -> Any:
    """The optional `bauta_rs` extension, or None if it isn't installed,
    BAUTA_NATIVE=0 turns it off, or its version isn't this package's.

    The two install separately, so nothing else stops a pairing whose masks
    differ -- which would reach a deployment as joins that quietly stop
    matching. Masking in Python instead is only slower.
    """

    if os.environ.get('BAUTA_NATIVE') == '0':
        return None

    try:
        import bauta_rs
    except ImportError:
        return None

    try:
        expected = importlib.metadata.version('bauta')
    except importlib.metadata.PackageNotFoundError:
        # Imported from a source tree that was never installed: nothing to compare against.
        return bauta_rs

    installed = getattr(bauta_rs, '__version__', None)
    if installed != expected:
        logging.getLogger(LOGGER_NAME).warning(
            'Masking in Python: bauta-rs %s does not match bauta %s, and only the same version is certain to mask identically. '
            'pip install "bauta[native]==%s" installs the matching one.', installed, expected, expected)
        return None

    return bauta_rs


def nativeVersion() -> Optional[str]:
    """The native masker's version, or None when masking runs in pure Python."""

    module = _nativeModule()

    return getattr(module, '__version__', None) if module is not None else None


MASKING_THREADS_VARIABLE = 'BAUTA_MASKING_THREADS'


def availableCores() -> int:
    """The cores this process may use. The native masker reads a container's
    CPU quota, which os.cpu_count() doesn't: in a pod limited to two CPUs on a
    64-core host, it says 2.
    """

    module = _nativeModule()
    if module is not None:
        return int(module.availableCores())

    return os.cpu_count() or 1


def effectiveMaskingThreads(setting: Union[str, int]) -> Union[str, int]:
    """The setting in force -- BAUTA_MASKING_THREADS over jobs.yaml's -- checked:
    `auto`, or a number from 1 to the cores available. Raises ValueError.
    """

    override = os.environ.get(MASKING_THREADS_VARIABLE)
    source = 'maskingThreads'
    if override:
        source = MASKING_THREADS_VARIABLE
        if override == 'auto':
            setting = override
        else:
            try:
                setting = int(override)
            except ValueError:
                raise ValueError('{} must be a number or auto, got {!r}'.format(MASKING_THREADS_VARIABLE, override)) from None

    if setting == 'auto':
        return setting

    cores = availableCores()
    if not isinstance(setting, int) or setting < 1:
        raise ValueError('{} must be at least 1, or auto; got {!r}'.format(source, setting))
    if setting > cores:
        raise ValueError('{} is {}, but this machine has {} core(s) available to it: set at most {}, or auto'.format(
            source, setting, cores, cores))

    return setting


def maskingThreadsFor(setting: Union[str, int], concurrentJobs: int) -> int:
    """How many threads a job masks with: a number as it is, or for `auto` the
    cores shared out between `concurrentJobs` -- a starting job and the ones
    running alongside it. BAUTA_MASKING_THREADS overrides the setting, which is
    checked by effectiveMaskingThreads.
    """

    setting = effectiveMaskingThreads(setting)
    if setting == 'auto':
        return max(1, availableCores() // max(1, concurrentJobs))

    return int(setting)


def setMaskingThreads(threads: int) -> None:
    """How many threads the native masker spreads a chunk over in this process.
    Results are identical for any count; pure-Python masking is always one.
    """

    module = _nativeModule()
    if module is not None:
        module.setThreads(threads)


def maskingImplementation() -> str:
    """Which implementation is masking, as one word, for the manifest and
    maskingIdentity.
    """

    native = nativeVersion()

    return 'bauta-rs/{}'.format(native) if native else 'python'


class MaskingError(Exception):
    """A policy that can't be applied: a column it doesn't cover, or a value of
    the wrong type for its strategy. Never retried. Messages name the value's
    type, never the value.
    """


def _canonical(value: Any) -> bytes:
    """The bytes a value is keyed on: numbers as plain decimal text and dates
    as ISO 8601, so the same id or date masks identically whichever driver
    returned it, and whether as a number or as text.
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


def maskingIdentity(key: str) -> str:
    """What a masked job records, so a later run can tell whether its masks
    still agree with its target's: the key's fingerprint and the implementation.

    The implementations are tested to agree byte for byte (mask-rs/vectors),
    but a divergence wouldn't change the fingerprint, so the implementation is
    recorded too. Space-separated because it shares one memory column with
    older state, which is a bare fingerprint.
    """

    return '{} {}'.format(keyFingerprint(key), maskingImplementation())


def splitMaskingIdentity(recorded: str) -> Tuple[str, Optional[str]]:
    """A recorded identity as (fingerprint, implementation). The implementation
    is None for state written before it was recorded.
    """

    fingerprint, _, implementation = recorded.partition(' ')

    return fingerprint, implementation or None


def keyFingerprint(key: str) -> str:
    """A short, non-reversible identifier for a key, safe to log and to record
    in a manifest.
    """

    return hmac.digest(key.encode('utf-8'), b'bauta key fingerprint', 'sha256')[:6].hex()


class KeyedHash:
    """HMAC-SHA256 under a per-domain subkey, derived once per column."""

    def __init__(self, key: str, domain: str) -> None:
        self._subkey = hmac.digest(key.encode('utf-8'), b'domain\x00' + domain.encode('utf-8'), 'sha256')
        # HMAC's two padded-key states, built once and copied per value: the
        # same digest in about a third less time. Only valid for a key shorter
        # than the block, hence the assert.
        assert len(self._subkey) == 32
        paddedKey = self._subkey.ljust(HMAC_BLOCK_SIZE, b'\x00')
        self._inner = hashlib.sha256(bytes(byte ^ 0x36 for byte in paddedKey))
        self._outer = hashlib.sha256(bytes(byte ^ 0x5c for byte in paddedKey))


    @property
    def subkey(self) -> bytes:
        """What a native masker is built from, one HMAC away from the key."""

        return self._subkey


    def digest(self, message: bytes, purpose: bytes = b'') -> bytes:

        inner = self._inner.copy()
        inner.update(purpose + b'\x00' + message)
        outer = self._outer.copy()
        outer.update(inner.digest())

        return outer.digest()


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
        """A keyed permutation of range(size).

        A balanced Feistel network over the smallest even bit width covering
        `size`, with HMAC as the round function, cycle-walked back into range.
        The walk always terminates and averages under four steps.
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


# What the extension puts in `problems` for a value it doesn't mask itself.
_NATIVE_FALLBACK = 'fallback'


class Strategy:
    """How one column is masked.

    Subclasses declare OPTIONS, as name -> check applied to the configured
    value, and implement mask() for one non-NULL value. NULL passes through
    unless a strategy overrides maskColumn.
    """

    NAME: ClassVar[str]
    OPTIONS: ClassVar[Dict[str, Callable[[Any], Any]]] = {}
    REQUIRED: ClassVar[Tuple[str, ...]] = ()
    # Whether the strategy uses the key at all; the manifest records it.
    KEYED: ClassVar[bool] = True
    # Whether mask() depends only on the value, key and options, so results
    # can be remembered.
    CACHEABLE: ClassVar[bool] = False
    # The native masker's name for this strategy, where it has one.
    NATIVE: ClassVar[Optional[str]] = None

    def __init__(self, keyedHash: KeyedHash, options: Mapping[str, Any]) -> None:
        self.keyedHash = keyedHash
        self.options = dict(options)
        self._cache: Dict[Tuple[type, Any], Any] = {}
        self._native = self._buildNative()


    def _buildNative(self) -> Any:
        """A native masker for this strategy and these options, or None -- also
        when an older extension doesn't know an option, so Python masks instead.
        """

        module = _nativeModule()
        if module is None or self.NATIVE is None:
            return None

        try:
            return module.Masker(self.keyedHash.subkey, self.NATIVE, self._nativeOptions())
        except (ValueError, TypeError):
            return None


    def _nativeOptions(self) -> Dict[str, Any]:
        """What the native masker is built from: the options, for most."""

        return self.options


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

        if self._native is not None:
            return self._maskColumnNatively(values)

        if not self.CACHEABLE:
            return [None if value is None else self.mask(value) for value in values]

        return [None if value is None else self._maskRemembered(value) for value in values]


    def _maskColumnNatively(self, values: Sequence[Any]) -> List[Any]:
        """The column through the extension, with Python finishing the values it
        doesn't cover. Resolved in order, so the first bad value raises whichever
        implementation ran.
        """

        masked, problems = self._native.maskColumn(list(values))

        for index in sorted(problems):
            problem = problems[index]
            if problem == _NATIVE_FALLBACK:
                value = values[index]
                masked[index] = None if value is None else self.mask(value)
            else:
                raise MaskingError(problem[1])

        return masked


    def _maskRemembered(self, value: Any) -> Any:
        """mask(), from the cache where the value's type allows."""

        kind = type(value)
        if kind not in _CACHEABLE_TYPES or (kind is str and len(value) > MASK_CACHE_MAXIMUM_TEXT):
            return self.mask(value)

        cacheKey = (kind, value)
        try:
            return self._cache[cacheKey]
        except KeyError:
            pass

        masked = self.mask(value)
        if len(self._cache) >= MASK_CACHE_SIZE:
            # Emptied, not evicted: the hot values are back within a chunk.
            self._cache.clear()
        self._cache[cacheKey] = masked

        return masked


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


def resolveStrategy(name: Any) -> Type[Strategy]:
    """A built-in strategy by name, or your own Strategy subclass as
    `module.path:ClassName`.
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
        raise ValueError('strategy {!r} is not a subclass of bauta.masking.Strategy'.format(name))

    return strategy

# Fields of a column policy that belong to the policy itself rather than to its
# strategy. Everything else in the mapping is a strategy option.
POLICY_FIELDS = ('strategy', 'domain')


def validateColumnPolicy(policy: Any) -> Dict[str, Any]:
    """One column's policy in mapping form, with its options checked. Accepts
    the shorthand `email: hash`. Raises ValueError.
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

    bind() raises unless every returned column is covered, by name or by
    defaultStrategy, and every named column is returned -- so a new production
    column stops the job instead of being copied unmasked. Names match
    case-insensitively, for Oracle. A domain defaults to the lower-cased
    column name.
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


    def apply(self, rows: Sequence[Tuple[Any, ...]], chunkIndex: Optional[int] = None) -> List[Tuple[Any, ...]]:
        """`chunkIndex` is the chunk's position in the source, which `shuffle`
        keys on. Left out, it counts calls.
        """

        if chunkIndex is None:
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
    """What was masked, how, and under which key, for an auditor. Never a value
    or the key.

    `declared` maps each masked job to its configured target and key
    fingerprint. A failed or skipped job is still listed, with no columns.
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

    # The implementation, for the reason given in maskingIdentity.
    manifest: Dict[str, Any] = {'generatedAt': timestamp.isoformat(timespec='seconds'), 'jobs': jobs}
    manifest['maskedBy'] = maskingImplementation()

    return manifest


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
    sorted, compact JSON.
    """

    body = {name: value for name, value in manifest.items() if name != INTEGRITY_FIELD}

    return json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=False, default=str).encode('utf-8')


def sealManifest(manifest: Mapping[str, Any], signingKey: Optional[str] = None) -> Dict[str, Any]:
    """The manifest with an `integrity` section: a SHA-256 digest and, given a
    signing key, an HMAC-SHA256 signature with the key's fingerprint.

    Passed through JSON first, so what is sealed is what a verifier reads back.
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
    """Checks a sealed manifest. A signature is only valid against the key
    whose fingerprint it records.
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
