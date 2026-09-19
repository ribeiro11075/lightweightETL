"""Deterministic, keyed masking, applied to rows on their way to the target.

Every masked value is derived from a keyed hash of the value itself, scoped by a
named *domain*:

    mask = strategy( HMAC(key, domain, value) )

So the same value in the same domain masks the same way in every table and on
every run, and nobody without the key can match candidate values to masks.

This module is what every strategy shares -- the keyed hash, the Strategy
base, the native masker -- and the plans that apply them; the strategies that
ship are in builtinMasking. Nothing here imports the rest of the package but
those and the logger's name. Values never appear in an error message or a log
line.
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


def resolveStrategy(name: Any) -> Type[Strategy]:
    """A built-in strategy by name, or your own Strategy subclass as
    `module.path:ClassName`.
    """

    from .builtinMasking import STRATEGIES

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
        from .builtinMasking import KeepStrategy

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


def __getattr__(name: str) -> Any:
    """The built-in strategies and their registry moved to builtinMasking, and
    the fake* lists to fakeData; `bauta.masking.STRATEGIES`, `LOCALES` and the
    like still find them. Imported on first use, since builtinMasking imports
    this module itself.
    """

    from . import builtinMasking, fakeData

    for module in (builtinMasking, fakeData):
        if hasattr(module, name):
            return getattr(module, name)

    raise AttributeError('module {!r} has no attribute {!r}'.format(__name__, name))
