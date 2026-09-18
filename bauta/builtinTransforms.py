"""Transformer functions that ship with the package.

Referenced from a job's sourceQueryColumnTransforms the same way any other
transformer is -- by import path -- so nothing here is privileged:

    sourceQueryColumnTransforms:
      amount:
      - bauta.builtinTransforms:currency
      name:
      - bauta.builtinTransforms:truncate(50)

Nothing here is privileged: your own module is referenced the same way.

Each passes NULL through -- except defaultIfNull -- and raises on a value it
can't convert rather than guessing.
"""
from __future__ import annotations

import datetime
import decimal
import json
import math
import re
import unicodedata
from typing import Any, Optional

_TRUE = frozenset({'y', 'yes', 'true', 't', '1', 'on'})
_FALSE = frozenset({'n', 'no', 'false', 'f', '0', 'off'})


def _blank(value: Any) -> bool:

    return isinstance(value, str) and not value.strip()


# Text -------------------------------------------------------------------------

def upper(value: Optional[str]) -> Optional[str]:

    return value.upper() if value is not None else value


def lower(value: Optional[str]) -> Optional[str]:

    return value.lower() if value is not None else value


def title(value: Optional[str]) -> Optional[str]:
    """`ann-marie o'neil` -> `Ann-Marie O'Neil`."""

    return value.title() if value is not None else value


def strip(value: Optional[str]) -> Optional[str]:

    return value.strip() if value is not None else value


def collapseWhitespace(value: Optional[str]) -> Optional[str]:
    """Strips, and turns every run of spaces, tabs and newlines into one space."""

    return ' '.join(value.split()) if value is not None else value


def removeAccents(value: Optional[str]) -> Optional[str]:
    """`Zoë Müller` -> `Zoe Muller`: drops combining marks after decomposing.
    Letters that aren't a base letter plus a mark (ß, ø, ł) are kept.
    """

    if value is None:
        return value

    return ''.join(character for character in unicodedata.normalize('NFKD', value) if not unicodedata.combining(character))


def truncate(value: Optional[str], maxLength: int = 255) -> Optional[str]:
    """At most maxLength characters: `truncate(50)` for a VARCHAR(50) target."""

    if maxLength < 0:
        raise ValueError('maxLength cannot be negative')

    return value[:maxLength] if value is not None else value


def padLeft(value: Any, width: int, fill: str = '0') -> Optional[str]:
    """Text of at least `width` characters, filled on the left: `padLeft(5)`
    turns 42 into `00042`. Longer values are left as they are.
    """

    if len(fill) != 1:
        raise ValueError('fill must be a single character')

    return str(value).rjust(width, fill) if value is not None else value


def replace(value: Optional[str], old: str, new: str = '') -> Optional[str]:
    """Every occurrence of `old` replaced: `replace('-')` drops hyphens."""

    return value.replace(old, new) if value is not None else value


def regexReplace(value: Optional[str], pattern: str, replacement: str = '') -> Optional[str]:
    """re.sub: `regexReplace('\\s+', ' ')`. Group references like `\\1` work
    in the replacement.
    """

    return re.sub(pattern, replacement, value) if value is not None else value


def digitsOnly(value: Optional[str]) -> Optional[str]:
    """Strips everything but digits -- e.g. normalizing "(555) 123-4567" to
    "5551234567" before loading into a column with no formatting of its own.
    """

    return re.sub(r'\D', '', value) if value is not None else value


# NULL handling -----------------------------------------------------------------

def nullIfBlank(value: Optional[str]) -> Optional[str]:
    """Common cleanup for source systems that store empty string instead of NULL."""

    return None if _blank(value) else value


def nullIf(value: Any, *sentinels: Any) -> Any:
    """NULL where the value is one of `sentinels`: `nullIf('N/A', 'n/a', -1)`."""

    return None if value in sentinels else value


def defaultIfNull(value: Any, default: Any) -> Any:
    """`default` where the value is NULL: `defaultIfNull('unknown')`."""

    return default if value is None else value


# Numbers -----------------------------------------------------------------------

def _decimal(value: Any) -> decimal.Decimal:
    """A number as an exact Decimal. A float goes through its shortest repr, so
    0.1 becomes Decimal('0.1') rather than its full binary expansion.
    """

    if isinstance(value, bool):
        raise ValueError('a boolean is not a number')
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, float):
        return decimal.Decimal(repr(value))
    if isinstance(value, int):
        return decimal.Decimal(value)
    if isinstance(value, str):
        try:
            return decimal.Decimal(value.strip())
        except decimal.InvalidOperation:
            raise ValueError('not a number') from None

    raise TypeError('cannot read a {} as a number'.format(type(value).__name__))


def currency(value: Any, symbol: str = '$', decimals: int = 2) -> Optional[str]:
    """`1234.5` -> `$1,234.50`, and `-5` -> `-$5.00`. `currency('€')` or
    `currency('¥', 0)` for others.

    NULL stays NULL: an unknown amount isn't zero.
    """

    if value is None:
        return value

    amount = _decimal(value)
    sign = '-' if amount < 0 else ''

    return '{}{}{:,.{}f}'.format(sign, symbol, abs(amount), decimals)


def roundNumber(value: Any, digits: int = 0) -> Any:
    """Rounded half away from zero -- what finance means by rounding, unlike
    Python's round(), which rounds half to even. Keeps the value's type; an
    integer is only changed by a negative `digits` (`roundNumber(-2)`: 1250 ->
    1300).
    """

    if value is None:
        return value

    step = decimal.Decimal(1).scaleb(-digits)
    rounded = _decimal(value).quantize(step, rounding=decimal.ROUND_HALF_UP)

    if isinstance(value, int):
        return int(rounded)
    if isinstance(value, float):
        return float(rounded)

    return rounded


def toInteger(value: Any) -> Optional[int]:
    """An int from text, a whole float or a whole Decimal. Blank text is NULL.

    A fractional value raises rather than being silently cut short; round it
    first if that's what you want.
    """

    if value is None or _blank(value):
        return None
    if isinstance(value, bool):
        raise ValueError('a boolean is not a number')
    if isinstance(value, int):
        return value

    number = _decimal(value)
    if not number.is_finite() or number != number.to_integral_value():
        raise ValueError('not a whole number')

    return int(number)


def toDecimal(value: Any) -> Optional[decimal.Decimal]:
    """An exact Decimal from text, an int or a float. Blank text is NULL."""

    if value is None or _blank(value):
        return None

    number = _decimal(value)
    if not number.is_finite():
        raise ValueError('not a finite number')

    return number


# Booleans ----------------------------------------------------------------------

def booleanToYN(value: Optional[bool]) -> Optional[str]:
    """For loading into a target column typed as a single character rather than a
    real boolean -- a common shape in older schemas.
    """

    if value is None:
        return value

    return 'Y' if value else 'N'


def toBoolean(value: Any) -> Optional[bool]:
    """A bool from Y/N, yes/no, true/false, t/f, on/off, 1/0 (any case) or the
    integers 1 and 0. Blank text is NULL; anything else raises.
    """

    if value is None or _blank(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUE:
            return True
        if text in _FALSE:
            return False

    raise ValueError('not a recognisable boolean')


# Dates and times -------------------------------------------------------------------

def epochSecondsToDate(value: Optional[int]) -> Optional[datetime.date]:
    """In UTC, not local time, so the result doesn't depend on the server."""

    if value is None:
        return value

    return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc).date()


def epochSecondsToDateTime(value: Optional[float]) -> Optional[datetime.datetime]:
    """A timezone-aware UTC datetime, for the same reason as epochSecondsToDate."""

    if value is None:
        return value

    return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc)


def epochMillisecondsToDateTime(value: Optional[float]) -> Optional[datetime.datetime]:
    """As epochSecondsToDateTime, for the milliseconds JavaScript and Java use."""

    if value is None:
        return value

    seconds, milliseconds = divmod(int(value), 1000)

    return datetime.datetime.fromtimestamp(seconds, tz=datetime.timezone.utc) + datetime.timedelta(milliseconds=milliseconds)


def parseDate(value: Any, format: str = '%Y-%m-%d') -> Optional[datetime.date]:
    """A date from text, by a strptime format: `parseDate('%d/%m/%Y')`.

    A date passes through, and a datetime loses its time -- so the transform
    still works where a driver already returns dates. Blank text is NULL.
    """

    if value is None or _blank(value):
        return None
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value

    return datetime.datetime.strptime(value.strip(), format).date()


def parseDateTime(value: Any, format: str = '%Y-%m-%d %H:%M:%S') -> Optional[datetime.datetime]:
    """A datetime from text, by a strptime format. A `%z` in the format keeps
    the offset. A datetime passes through, and a date becomes its midnight.
    Blank text is NULL.
    """

    if value is None or _blank(value):
        return None
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time())

    return datetime.datetime.strptime(value.strip(), format)


def formatDate(value: Any, format: str = '%Y-%m-%d') -> Optional[str]:
    """A date or datetime as text, by a strftime format: `formatDate('%Y%m')`."""

    if value is None:
        return value
    if not isinstance(value, (datetime.date, datetime.time)):
        raise TypeError('formatDate needs a date, datetime or time, got {}'.format(type(value).__name__))

    return value.strftime(format)


# Conversions -------------------------------------------------------------------

def toString(value: Any) -> Optional[str]:
    """Text for a text column. Dates and times become ISO 8601, bytes are read
    as UTF-8, and floats keep their shortest exact form.
    """

    if value is None or isinstance(value, str):
        return value
    if isinstance(value, datetime.datetime):
        return value.isoformat(sep=' ')
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode('utf-8')
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError('not a finite number')

    return str(value)


def toJson(value: Any) -> Optional[str]:
    """JSON text, for loading a document or array into a text column -- what
    PostgreSQL's json columns and MySQL's JSON come back as. Text passes
    through, on the assumption it's JSON already. Keys are sorted, so the same
    document always gives the same text.
    """

    if value is None or isinstance(value, str):
        return value

    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
