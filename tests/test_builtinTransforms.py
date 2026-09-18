"""The transformers that ship with the package, called directly and through the
references a job configuration uses.
"""
import datetime
import decimal
import inspect

import pytest

from bauta import builtinTransforms
from bauta.transform import TransformResolutionError, resolveTransformer

PUBLIC = sorted(name for name, function in vars(builtinTransforms).items()
                if inspect.isfunction(function) and function.__module__ == builtinTransforms.__name__ and not name.startswith('_'))

# The ones that need an argument besides the value, with arguments that work.
ARGUMENTS = {'padLeft': '(5)', 'replace': "('-')", 'regexReplace': "('x')", 'defaultIfNull': "('unknown')"}

UTC = datetime.timezone.utc


@pytest.mark.parametrize('reference,value,expected', [
    ('currency', 1234.5, '$1,234.50'),
    ('currency', decimal.Decimal('-5'), '-$5.00'),
    ('currency', '0.125', '$0.12'),
    ("currency('€')", 3, '€3.00'),
    ("currency('¥', 0)", 1234.6, '¥1,235'),
    ('upper', 'abc', 'ABC'),
    ('lower', 'ABC', 'abc'),
    ('title', "ann-marie o'neil", "Ann-Marie O'Neil"),
    ('strip', '  padded  ', 'padded'),
    ('collapseWhitespace', '  a \t b\n\nc ', 'a b c'),
    ('removeAccents', 'Zoë Müller-Ñúñez', 'Zoe Muller-Nunez'),
    ('truncate(3)', 'abcdef', 'abc'),
    ('truncate(3)', 'ab', 'ab'),
    ('truncate(maxLength=2)', 'abc', 'ab'),
    ('truncate', 'x' * 300, 'x' * 255),
    ('padLeft(5)', 42, '00042'),
    ("padLeft(4, ' ')", 'ab', '  ab'),
    ('padLeft(2)', 'abcd', 'abcd'),
    ("replace('-')", '555-0100', '5550100'),
    ("replace('-', '.')", '555-0100', '555.0100'),
    (r"regexReplace('\\s+', ' ')", 'a   b', 'a b'),
    (r"regexReplace('(\\d{3})(\\d{4})', '\\1-\\2')", '5550100', '555-0100'),
    ('digitsOnly', '+1 (555) 010-9999', '15550109999'),
    ('nullIfBlank', '   ', None),
    ('nullIfBlank', 'kept', 'kept'),
    ("nullIf('N/A', 'n/a', -1)", 'n/a', None),
    ("nullIf('N/A', 'n/a', -1)", -1, None),
    ("nullIf('N/A')", 'kept', 'kept'),
    ("defaultIfNull('unknown')", None, 'unknown'),
    ("defaultIfNull('unknown')", 'known', 'known'),
    ('roundNumber', 2.5, 3.0),
    ('roundNumber(2)', decimal.Decimal('1.005'), decimal.Decimal('1.01')),
    ('roundNumber(1)', -0.25, -0.3),
    ('roundNumber(-2)', 1250, 1300),
    ('roundNumber', 7, 7),
    ('toInteger', ' 42 ', 42),
    ('toInteger', 3.0, 3),
    ('toInteger', decimal.Decimal('12.00'), 12),
    ('toInteger', '', None),
    ('toDecimal', '0.1', decimal.Decimal('0.1')),
    ('toDecimal', 0.1, decimal.Decimal('0.1')),
    ('toDecimal', 5, decimal.Decimal(5)),
    ('booleanToYN', True, 'Y'),
    ('booleanToYN', False, 'N'),
    ('toBoolean', ' Yes ', True),
    ('toBoolean', 'f', False),
    ('toBoolean', 1, True),
    ('toBoolean', '', None),
    ('epochSecondsToDate', 0, datetime.date(1970, 1, 1)),
    ('epochSecondsToDateTime', 90061.5, datetime.datetime(1970, 1, 2, 1, 1, 1, 500000, tzinfo=UTC)),
    ('epochMillisecondsToDateTime', 1_700_000_000_123, datetime.datetime(2023, 11, 14, 22, 13, 20, 123000, tzinfo=UTC)),
    ("parseDate('%d/%m/%Y')", ' 31/12/2026 ', datetime.date(2026, 12, 31)),
    ('parseDate', datetime.datetime(2026, 1, 2, 3, 4), datetime.date(2026, 1, 2)),
    ('parseDate', '', None),
    ('parseDateTime', '2026-01-02 03:04:05', datetime.datetime(2026, 1, 2, 3, 4, 5)),
    ("parseDateTime('%Y-%m-%dT%H:%M%z')", '2026-01-02T03:04+0200',
     datetime.datetime(2026, 1, 2, 3, 4, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))),
    ('parseDateTime', datetime.date(2026, 1, 2), datetime.datetime(2026, 1, 2)),
    ("formatDate('%Y%m')", datetime.date(2026, 3, 9), '202603'),
    ('formatDate', datetime.datetime(2026, 3, 9, 10, 0), '2026-03-09'),
    ('toString', datetime.datetime(2026, 3, 9, 10, 0), '2026-03-09 10:00:00'),
    ('toString', b'caf\xc3\xa9', 'café'),
    ('toString', 0.1, '0.1'),
    ('toString', decimal.Decimal('1.50'), '1.50'),
    ('toJson', {'b': [1, 2], 'a': 'é'}, '{"a": "é", "b": [1, 2]}'),
    ('toJson', '{"already": "json"}', '{"already": "json"}'),
    ])
def test_a_stock_transformer_produces_the_expected_value(reference, value, expected):
    transformer = resolveTransformer('bauta.builtinTransforms:' + reference)

    assert transformer(value) == expected
    assert type(transformer(value)) is type(expected)


@pytest.mark.parametrize('reference,value', [
    ('toInteger', '1.5'),
    ('toInteger', 2.5),
    ('toInteger', True),
    ('toInteger', 'abc'),
    ('toDecimal', 'NaN'),
    ('toBoolean', 'maybe'),
    ('toBoolean', 2),
    ('currency', 'abc'),
    ('currency', True),
    ('formatDate', '2026-01-01'),
    ("parseDate('%d/%m/%Y')", '2026-01-01'),
    ('truncate(-1)', 'abc'),
    ("padLeft(3, 'ab')", 'x'),
    ('toString', float('nan')),
    ])
def test_a_value_that_cannot_be_converted_raises_rather_than_being_guessed(reference, value):
    with pytest.raises((ValueError, TypeError)):
        resolveTransformer('bauta.builtinTransforms:' + reference)(value)


@pytest.mark.parametrize('name', PUBLIC)
def test_every_stock_transformer_passes_null_through(name):
    """A NULL column value reaches a transformer like any other. None of these
    may raise on it, and none may invent a value for it -- currency used to turn
    NULL into $0.00. defaultIfNull is the one whose job is to replace it.
    """
    transformer = resolveTransformer('bauta.builtinTransforms:{}{}'.format(name, ARGUMENTS.get(name, '')))

    assert transformer(None) == ('unknown' if name == 'defaultIfNull' else None)


@pytest.mark.parametrize('name', sorted(ARGUMENTS))
def test_a_transformer_that_needs_arguments_is_refused_without_them(name):
    """Caught when the configuration is validated, not on the first row."""
    with pytest.raises(TransformResolutionError, match='missing a required argument'):
        resolveTransformer('bauta.builtinTransforms:' + name)


def test_the_documented_list_covers_every_stock_transformer():
    from pathlib import Path

    reference = (Path(__file__).resolve().parents[1] / 'docs' / 'configuration.md').read_text()

    assert [name for name in PUBLIC if '`{}'.format(name) not in reference] == []


def test_epoch_conversion_is_utc_not_local_time(monkeypatch):
    """Otherwise the same job produces different dates on servers in different
    timezones -- epoch 0 is 1970-01-01 in UTC but 1969-12-31 west of it.
    """
    import time

    monkeypatch.setenv('TZ', 'America/Los_Angeles')
    time.tzset()
    try:
        assert builtinTransforms.epochSecondsToDate(0) == datetime.date(1970, 1, 1)
    finally:
        monkeypatch.delenv('TZ', raising=False)
        time.tzset()
