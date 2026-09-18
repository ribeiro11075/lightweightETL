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
import random
import uuid

import pytest

from understudy_data.masking import STRATEGIES, KeyedHash, MaskingError, nativeVersion

native = pytest.mark.skipif(nativeVersion() is None, reason='the understudy_mask extension is not installed')

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
    ]


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
    import understudy_data.masking as masking

    monkeypatch.setenv('UNDERSTUDY_NATIVE', '0')
    masking._nativeModule.cache_clear()

    try:
        assert masking.nativeVersion() is None
        assert STRATEGIES['key'](KeyedHash(KEY, 'off'), {})._native is None
    finally:
        # Or every later test would find the extension off.
        masking._nativeModule.cache_clear()


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
