"""The stock transformers now ship with the package, so they need their own
coverage rather than being exercised incidentally by one integration test.
"""
import datetime

import pytest

from lightweight_etl import builtinTransforms
from lightweight_etl.transform import resolveTransformer


@pytest.mark.parametrize('name,value,expected', [
    ('currency', 1234.5, '$1,234.50'),
    ('currency', None, '$0.00'),
    ('upper', 'abc', 'ABC'),
    ('lower', 'ABC', 'abc'),
    ('strip', '  padded  ', 'padded'),
    ('nullIfBlank', '   ', None),
    ('nullIfBlank', 'kept', 'kept'),
    ('digitsOnly', '+1 (555) 010-9999', '15550109999'),
    ('booleanToYN', True, 'Y'),
    ('booleanToYN', False, 'N'),
    ('epochSecondsToDate', 0, datetime.date(1970, 1, 1)),
    ])
def test_a_stock_transformer_produces_the_expected_value(name, value, expected):
    assert getattr(builtinTransforms, name)(value) == expected


@pytest.mark.parametrize('name', ['currency', 'upper', 'lower', 'strip', 'truncate', 'nullIfBlank', 'digitsOnly', 'epochSecondsToDate', 'booleanToYN'])
def test_every_stock_transformer_passes_none_through_or_handles_it(name):
    """A NULL column value reaches a transformer like any other. None of these
    may raise on it -- a TransformError on every null row would make them
    unusable against real data.
    """
    getattr(builtinTransforms, name)(None)


@pytest.mark.parametrize('name', ['currency', 'upper', 'lower', 'strip', 'truncate', 'nullIfBlank', 'digitsOnly', 'epochSecondsToDate', 'booleanToYN'])
def test_every_stock_transformer_is_reachable_by_the_reference_a_config_would_use(name):
    assert callable(resolveTransformer('lightweight_etl.builtinTransforms:{}'.format(name)))


def test_truncate_shortens_and_leaves_short_values_alone():
    assert builtinTransforms.truncate('abcdef', maxLength=3) == 'abc'
    assert builtinTransforms.truncate('ab', maxLength=3) == 'ab'


def test_epoch_conversion_is_utc_not_local_time(monkeypatch):
    """Otherwise the same job produces different dates on servers in different
    timezones -- epoch 0 is 1970-01-01 in UTC but 1969-12-31 west of it.
    """
    import os
    import time

    monkeypatch.setenv('TZ', 'America/Los_Angeles')
    time.tzset()
    try:
        assert builtinTransforms.epochSecondsToDate(0) == datetime.date(1970, 1, 1)
    finally:
        monkeypatch.delenv('TZ', raising=False)
        time.tzset()
