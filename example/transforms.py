"""Transformer functions used by example_jobs.yaml's columnTransforms.

Referenced from job config as "example.transforms:currency" and resolved
at run time by library.resolveTransformer -- a library consumer's own
Transformer functions can live anywhere importable, not just here.
"""
from __future__ import annotations

from typing import Optional


def currency(value: Optional[float]) -> str:

    return '${:,.2f}'.format(value if value else 0)
