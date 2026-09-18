"""The native masker against the Python one, over the values that break things.

The extension is optional and its masks must be indistinguishable from Python's:
a difference isn't a wrong answer, it's a silent key change, and it would reach a
deployment as joins that quietly stop matching. mask-rs/vectors/reference.json
pins the Rust side from below; this pins the whole path, through the conversions
and the fallbacks, from above.

Skipped entirely when the extension isn't installed, which is also how the rest
of the suite runs in that configuration.
"""
import datetime
import decimal
import importlib.metadata
import logging
import random
import sys
import types
import uuid

import pytest

from bauta.masking import STRATEGIES, KeyedHash, MaskingError, nativeVersion

native = pytest.mark.skipif(nativeVersion() is None, reason='the bauta_rs extension is not installed')

KEY = 'a-test-key-that-is-long-enough'
ALPHANUMERIC = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'

# Shaped rather than merely plentiful. Random text would essentially never land
# on the boundaries that matter: 2**128 for a UUID domain, the 21-to-22
# character step where a Feistel half stops fitting 128 bits, MAXIMUM_KEY_LENGTH
# at 256, and the byte edges where a half outgrows its serialised width.
def corpus():
    random.seed(20260917)
    values = [None, True, False, 0, 1, -1, 9, 10, -10, 99, -99, 100]

    for length in (1, 2, 3, 5, 6, 7, 11, 16, 19, 20, 21, 22, 31, 32, 33, 40, 63, 64, 65, 255, 256, 257):
        values.append(''.join(random.choice(ALPHANUMERIC) for _ in range(length)))
        values.append(''.join(random.choice('0123456789') for _ in range(length)))
        values.append(''.join(random.choice('0123456789abcdef') for _ in range(length)))
        values.append(''.join(random.choice('0123456789ABCDEF') for _ in range(length)))
        values.append(''.join(random.choice('0123456789aBcDeF') for _ in range(length)))

    for power in (1, 2, 6, 7, 9, 11, 15, 16, 18, 19, 20, 38, 39, 40, 128, 255, 256, 257):
        values.append(10 ** power)
        values.append(10 ** power - 1)
        values.append(-(10 ** power))

    values += [
        uuid.uuid4(), uuid.UUID(int=0), uuid.UUID(int=(1 << 128) - 1),
        decimal.Decimal('42'), decimal.Decimal('-42'), decimal.Decimal('42.5'), decimal.Decimal('4.2E+3'),
        42.0, 42.5, datetime.date(2020, 2, 29), datetime.datetime(2020, 1, 1, 12, 30),
        'héllo', 'Ωmega', '٣٤٥', 'user@example.com', 'Alice.Smith@Corp.COM ', '  spaced  ',
        'alice@corp.com\x1c', '+1 (555) 010-9999', '', ' ', 'x', b'bytes',
        ]

    return values


VALUES = corpus()

COMBINATIONS = [
    ('key', {}), ('key', {'charset': 'hex'}), ('key', {'charset': 'digits'}),
    ('fpe', {}), ('fpe', {'charset': 'hex'}), ('fpe', {'charset': 'digits'}), ('fpe', {'strict': True}),
    ('hash', {}), ('hash', {'length': 12}), ('hash', {'length': 64, 'prefix': 'cust_'}),
    ('email', {}), ('email', {'keepDomain': True}), ('email', {'length': 8}),
    ('email', {'mailDomain': 'masked.invalid'}),
    ('digits', {}), ('digits', {'keepLeading': 2}), ('digits', {'keepTrailing': 4}),
    ('digits', {'keepLeading': 1, 'keepTrailing': 1}),
    ] + [(name, options) for name in ('fakeFirstName', 'fakeLastName', 'fakeName', 'fakeCity', 'fakeCompany', 'fakeStreetAddress')
         for options in ({}, {'maxLength': 3}, {'locale': 'de_DE'}, {'locale': 'pt_BR'})]


def outcome(built, values):
    """Each value's mask, or the error it raised -- both have to agree."""

    results = []
    for value in values:
        try:
            results.append(('ok', built.maskColumn([value], 0)[0]))
        except MaskingError as error:
            results.append(('error', str(error)))

    return results


@native
@pytest.mark.parametrize('name,options', COMBINATIONS, ids=lambda item: str(item))
def test_native_and_python_agree(name, options):
    validated = STRATEGIES[name].validateOptions(options)

    asNative = STRATEGIES[name](KeyedHash(KEY, 'agree'), validated)
    assert asNative._native is not None, '{} should have a native masker'.format(name)

    asPython = STRATEGIES[name](KeyedHash(KEY, 'agree'), validated)
    asPython._native = None

    fromNative = outcome(asNative, VALUES)
    fromPython = outcome(asPython, VALUES)

    for value, left, right in zip(VALUES, fromNative, fromPython):
        assert left == right, 'native and python disagree on {!r}: {!r} vs {!r}'.format(value, left, right)
        if left[0] == 'ok' and left[1] is not None:
            assert type(left[1]) is type(right[1]), 'type differs for {!r}'.format(value)


@native
@pytest.mark.parametrize('name,options', COMBINATIONS, ids=lambda item: str(item))
def test_a_whole_column_agrees_with_one_value_at_a_time(name, options):
    """Batching must not change an answer: the extension deduplicates within a
    column, so a repeated value has to mask as it would on its own.
    """

    validated = STRATEGIES[name].validateOptions(options)
    built = STRATEGIES[name](KeyedHash(KEY, 'agree'), validated)

    maskable = [value for value in VALUES if outcome(built, [value])[0][0] == 'ok']
    repeated = maskable + maskable

    assert built.maskColumn(repeated, 0) == [built.maskColumn([value], 0)[0] for value in repeated]


@native
def test_the_extension_can_be_turned_off(monkeypatch):
    import bauta.masking as masking

    monkeypatch.setenv('BAUTA_NATIVE', '0')
    masking._nativeModule.cache_clear()

    try:
        assert masking.nativeVersion() is None
        assert STRATEGIES['key'](KeyedHash(KEY, 'off'), {})._native is None
    finally:
        # Or every later test would find the extension off.
        masking._nativeModule.cache_clear()


@pytest.fixture
def standInExtension(monkeypatch):
    """A module in bauta_rs's place, whatever is installed, and the warnings
    the package logs while it's there. The package's logger doesn't propagate,
    so caplog wouldn't see them.
    """
    import bauta.masking as masking

    extension = types.ModuleType('bauta_rs')
    monkeypatch.setitem(sys.modules, 'bauta_rs', extension)
    monkeypatch.delenv('BAUTA_NATIVE', raising=False)

    warnings = []
    handler = logging.Handler(level=logging.WARNING)
    handler.emit = lambda record: warnings.append(record.getMessage())
    logging.getLogger('bauta').addHandler(handler)
    masking._nativeModule.cache_clear()

    yield extension, warnings

    logging.getLogger('bauta').removeHandler(handler)
    masking._nativeModule.cache_clear()


def test_an_extension_of_the_same_version_is_used(standInExtension):
    import bauta.masking as masking

    extension, warnings = standInExtension
    extension.__version__ = importlib.metadata.version('bauta')

    assert masking._nativeModule() is extension
    assert warnings == []


@pytest.mark.parametrize('installed', ['0.0.1', None], ids=['another version', 'no version'])
def test_an_extension_of_another_version_is_ignored_with_a_warning(standInExtension, installed):
    """The two install separately, so a mismatched pair is one pip command
    away, and nothing but this check stops it masking differently.
    """
    import bauta.masking as masking

    extension, warnings = standInExtension
    if installed is not None:
        extension.__version__ = installed

    assert masking._nativeModule() is None
    assert masking.maskingImplementation() == 'python'
    (warning,) = warnings
    assert 'bauta-rs {} does not match bauta {}'.format(installed, importlib.metadata.version('bauta')) in warning


@native
def test_the_masking_key_never_reaches_the_extension():
    """The extension is built from the per-domain subkey, which is already one
    HMAC away from the key. security.md is deliberate that the key stays out of
    anything that could end up in a repr or a traceback.
    """

    keyedHash = KeyedHash(KEY, 'secrecy')
    built = STRATEGIES['key'](keyedHash, {})

    assert KEY.encode() not in keyedHash.subkey
    assert KEY not in repr(built._native)


# Threads and the cross-chunk cache ---------------------------------------------

def _wideColumn(built):
    """Well past the size a call is split across threads at, with every value
    of the corpus the strategy masks -- the ones Python finishes included --
    and repeats both within and across chunks. Refusals are left to their own
    test: one would fail its whole chunk, and hide the masks beside it.
    """
    random.seed(20260918)
    distinct = ['C{:07d}'.format(number) for number in range(3000)] + list(range(10 ** 9, 10 ** 9 + 1500))
    maskable = [value for value in VALUES if outcome(built, [value])[0][0] == 'ok']
    column = [random.choice(distinct) for _ in range(6000)] + maskable * 3
    random.shuffle(column)

    return [value for value in column if outcome(built, [value])[0][0] == 'ok']


@pytest.fixture
def maskingThreads():
    """Sets the extension's threads, and puts them back to one after."""
    import bauta_rs

    yield bauta_rs.setThreads
    bauta_rs.setThreads(1)


@native
@pytest.mark.parametrize('name,options', [('key', {}), ('fpe', {}), ('hash', {}), ('digits', {'keepTrailing': 2}), ('email', {}),
                                          ('fakeName', {'locale': 'es_ES'}), ('fakeStreetAddress', {})],
                         ids=lambda item: str(item))
def test_threads_and_the_cache_change_no_answer(name, options, maskingThreads):
    """Every mask depends on its value alone, so masking a column on eight
    threads, chunk after chunk with the cache warm, must give exactly what one
    thread and pure Python give -- errors included, in the same place.
    """
    validated = STRATEGIES[name].validateOptions(options)
    column = _wideColumn(STRATEGIES[name](KeyedHash(KEY, 'threads'), validated))
    chunks = [column[start:start + 2000] for start in range(0, len(column), 2000)]

    def maskAll(threads, native=True):
        maskingThreads(threads)
        built = STRATEGIES[name](KeyedHash(KEY, 'threads'), validated)
        if not native:
            built._native = None
        return [_chunkOutcome(built, chunk, index) for index, chunk in enumerate(chunks)]

    oneThread = maskAll(1)

    assert all(result == 'ok' for result, _ in oneThread)
    # Enough distinct values in each call that it's split across the threads.
    assert min(len({repr(value) for value in chunk}) for chunk in chunks) > 256
    assert maskAll(8) == oneThread
    assert maskAll(8) == maskAll(1, native=False)


def _chunkOutcome(built, chunk, index):
    """A chunk's masks, or the first error it raised."""
    try:
        return ('ok', built.maskColumn(chunk, index))
    except MaskingError as error:
        return ('error', str(error))


@native
def test_a_refusal_is_not_remembered(maskingThreads):
    """Only answers are cached, so a value refused in one chunk is refused
    again in the next rather than answered from the cache.
    """
    maskingThreads(8)
    built = STRATEGIES['key'](KeyedHash(KEY, 'refusals'), STRATEGIES['key'].validateOptions({}))

    for index in range(2):
        with pytest.raises(MaskingError):
            built.maskColumn(['fine'] * 300 + [True], index)


@native
def test_the_cache_is_bounded_and_keeps_answering_past_its_bound(maskingThreads):
    maskingThreads(4)
    built = STRATEGIES['key'](KeyedHash(KEY, 'bounded'), STRATEGIES['key'].validateOptions({}))
    values = ['V{:06d}'.format(number) for number in range(70_000)]

    first = built.maskColumn(values, 0)

    assert built.maskColumn(values[::-1], 1) == first[::-1]
    assert len(set(first)) == len(values)


def test_auto_shares_the_cores_between_the_jobs_that_run_at_once(monkeypatch):
    import bauta.masking as masking

    monkeypatch.delenv(masking.MASKING_THREADS_VARIABLE, raising=False)
    monkeypatch.setattr(masking, 'availableCores', lambda: 8)

    assert masking.maskingThreadsFor('auto', 1) == 8
    assert masking.maskingThreadsFor('auto', 3) == 2
    assert masking.maskingThreadsFor('auto', 16) == 1
    assert masking.maskingThreadsFor(3, 4) == 3

    monkeypatch.setenv(masking.MASKING_THREADS_VARIABLE, '5')
    assert masking.maskingThreadsFor('auto', 1) == 5
    monkeypatch.setenv(masking.MASKING_THREADS_VARIABLE, 'auto')
    assert masking.maskingThreadsFor(2, 2) == 4


@pytest.mark.parametrize('setting,valid', [('auto', True), (1, True), (12, True), (0, False), (-1, False), ('many', False)])
def test_masking_threads_is_auto_or_at_least_one(setting, valid):
    from bauta.configuration import Configuration, ConfigurationError, DataJobsFile

    raw = {'workers': 1, 'maskingThreads': setting, 'jobs': {}}
    if valid:
        assert Configuration.validateJobConfiguration(raw, DataJobsFile).maskingThreads == setting
    else:
        with pytest.raises(ConfigurationError, match='maskingThreads'):
            Configuration.validateJobConfiguration(raw, DataJobsFile)
