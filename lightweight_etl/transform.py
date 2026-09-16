from __future__ import annotations

import importlib
from typing import Any, Callable, Dict, List, Optional, Tuple

Transformer = Callable[[Any], Any]


class TransformResolutionError(Exception):
    """Raised when a "module.path:function_name" transformer reference can't be resolved."""


class TransformError(Exception):
    """Raised when applying a resolved transformer actually fails.

    Two distinct cases surface here, both eagerly rather than as a bare traceback
    deep inside a row loop: (1) columnTransforms names a column that isn't in
    `columns` at all -- for lightweight-etl's own job-running path (see
    runner.py's _executeDataJob), `columns` there is sourceQuery's own result
    columns (from cursor.description -- whatever it actually selected, explicit
    list or `select *` alike), not the target table, since a transform runs on a
    value as extracted from the source, before it's mapped onto any target column
    name -- checked up front, before touching any row, so it fails the same way
    every time instead of being silently skipped; (2) a transformer raises on a
    particular value (e.g. a `str`-only transform handed a row where that column
    is an int) -- caught per-value and re-raised with the column name and
    offending value attached, since the original exception alone doesn't say
    which column/value caused it.
    """


def resolveTransformer(reference: str) -> Transformer:
    """Import a Transformer from a "module.path:function_name" reference.

    Lets a job configuration name a function defined anywhere importable --
    lightweight_etl/builtinTransforms.py, or any module of the user's own -- without the caller
    having to pre-register it in a lookup table.
    """

    modulePath, separator, attributeName = reference.partition(':')

    if not separator:
        raise TransformResolutionError(f'transformer reference "{reference}" must be in the form "module.path:function_name"')

    try:
        module = importlib.import_module(modulePath)
    except ImportError as error:
        raise TransformResolutionError(f'transformer reference "{reference}": no module named "{modulePath}"') from error

    transformer = getattr(module, attributeName, None)

    if transformer is None:
        raise TransformResolutionError(f'transformer reference "{reference}": "{modulePath}" has no attribute "{attributeName}"')
    if not callable(transformer):
        raise TransformResolutionError(f'transformer reference "{reference}": "{attributeName}" is not callable')

    return transformer


class Transform:
    """Applies per-column transformers to rows.

    Split into validate()/apply() so a streaming job can check its configuration
    once, up front, and then transform an unbounded number of chunks without
    re-checking anything. transform() is the whole-dataset convenience that does
    both, and `data` is optional precisely so the streaming path can build one of
    these from columns alone, before a single row has been fetched.
    """

    def __init__(self, columns: List[str], columnTransforms: Dict[str, List[Transformer]],
                  data: Optional[List[Tuple[Any, ...]]] = None) -> None:
        self.columns = columns
        self.columnTransforms = columnTransforms
        self.data = data if data is not None else []
        self._transformsByIndex = [(index, self.columnTransforms.get(column, [])) for index, column in enumerate(self.columns)]


    def validate(self) -> None:
        """Raises TransformError if any transform names a column that isn't there.

        Kept separate from apply() so it can run *before the first write*, which
        is the guarantee that matters: a streaming job that validated per-chunk
        would already have loaded rows into the target by the time it noticed a
        misconfigured column name.
        """

        unknownColumns = [column for column in self.columnTransforms if column not in self.columns]

        if unknownColumns:
            raise TransformError('sourceQueryColumnTransforms references column(s) not present in columns {}: {}'.format(self.columns, ', '.join(unknownColumns)))


    def apply(self, data: List[Tuple[Any, ...]]) -> List[Tuple[Any, ...]]:
        """Transforms one batch of rows. Assumes validate() has already run."""

        if not data:
            return data

        rows = [list(row) for row in data]

        for index, transformers in self._transformsByIndex:
            for transformer in transformers:
                for row in rows:
                    value = row[index]
                    try:
                        row[index] = transformer(value)
                    except Exception as error:
                        transformerName = getattr(transformer, '__name__', repr(transformer))
                        raise TransformError('transformer "{}" failed on column "{}" for value {!r}: {}'.format(
                            transformerName, self.columns[index], value, error)) from error

        return [tuple(row) for row in rows]


    def transform(self) -> List[Tuple[Any, ...]]:
        """Validate and transform the whole of `data` in one call."""

        self.validate()

        return self.apply(self.data)
