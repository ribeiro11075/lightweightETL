"""Transformer functions that ship with the package.

Referenced from a job's sourceQueryColumnTransforms the same way any other
transformer is -- by import path -- so nothing here is privileged:

    sourceQueryColumnTransforms:
      amount:
      - lightweight_etl.builtinTransforms:currency

These are the handful that come up in nearly every load. resolveTransformer
takes any importable "module.path:function_name", so your own module sits
alongside these rather than replacing them; see example/ for one written
outside the package.

The name is plural and prefixed to keep it distinct from transform.py, which
holds the machinery that *applies* transformers rather than any particular one.
"""
from __future__ import annotations

import datetime
import re
from typing import Optional


def currency(value: Optional[float]) -> str:

    return '${:,.2f}'.format(value if value else 0)


def upper(value: Optional[str]) -> Optional[str]:

    return value.upper() if value is not None else value


def lower(value: Optional[str]) -> Optional[str]:

    return value.lower() if value is not None else value


def strip(value: Optional[str]) -> Optional[str]:

    return value.strip() if value is not None else value


def truncate(value: Optional[str], maxLength: int = 255) -> Optional[str]:
    """Not directly usable as a Transformer reference as-is (it takes two
    arguments, and resolveTransformer's contract is one column value in); wrap it
    with functools.partial in your own module, e.g.
    `functools.partial(truncate, maxLength=50)`, and reference *that*.
    """

    return value[:maxLength] if value is not None else value


def nullIfBlank(value: Optional[str]) -> Optional[str]:
    """Common cleanup for source systems that store empty string instead of NULL."""

    return None if value is not None and value.strip() == '' else value


def digitsOnly(value: Optional[str]) -> Optional[str]:
    """Strips everything but digits -- e.g. normalizing "(555) 123-4567" to
    "5551234567" before loading into a column with no formatting of its own.
    """

    return re.sub(r'\D', '', value) if value is not None else value


def epochSecondsToDate(value: Optional[int]) -> Optional[datetime.date]:
    """UTC, not the machine's local timezone.

    date.fromtimestamp() converts in local time, which would make the same job
    produce different dates depending on which server it ran on -- epoch 0 is
    1970-01-01 in UTC but 1969-12-31 anywhere west of it. A value loaded into a
    warehouse must not depend on a worker's TZ setting.
    """

    if value is None:
        return value

    return datetime.datetime.fromtimestamp(value, tz=datetime.timezone.utc).date()


def booleanToYN(value: Optional[bool]) -> Optional[str]:
    """For loading into a target column typed as a single character rather than a
    real boolean -- a common shape in older schemas.
    """

    if value is None:
        return value

    return 'Y' if value else 'N'
