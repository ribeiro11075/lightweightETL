"""Transformer functions used by example_jobs.yaml's sourceQueryColumnTransforms.

Referenced from job config as "example.example_transforms:currency" and resolved
at run time by library.resolveTransformer -- a library consumer's own
Transformer functions can live anywhere importable, not just here.

A Transformer is any Callable[[Any], Any]: it receives one column's raw value
from one row and returns the value to write in its place. Each is applied to
every row's value for that column, in the order listed in
sourceQueryColumnTransforms -- so `[a, b]` means "apply a, then apply b's result
to a's result", not "apply both to the original value".
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

    return datetime.date.fromtimestamp(value) if value is not None else value


def booleanToYN(value: Optional[bool]) -> Optional[str]:
    """For loading into a target column typed as a single character rather than a
    real boolean -- a common shape in older schemas.
    """

    if value is None:
        return value

    return 'Y' if value else 'N'
