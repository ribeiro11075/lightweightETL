from __future__ import annotations

import importlib
from typing import Any, Callable, Dict, List, Tuple

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
    example/example_transforms.py, or any module of the user's own -- without the caller
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

    def __init__(self, data: List[Tuple[Any, ...]], columns: List[str], columnTransforms: Dict[str, List[Transformer]]) -> None:
        self.data = data
        self.columns = columns
        self.columnTransforms = columnTransforms


    def transform(self) -> List[Tuple[Any, ...]]:

        unknownColumns = [column for column in self.columnTransforms if column not in self.columns]
        if unknownColumns:
            raise TransformError('sourceQueryColumnTransforms references column(s) not present in columns {}: {}'.format(self.columns, ', '.join(unknownColumns)))

        if not len(self.data):
            return self.data

        rows = [list(row) for row in self.data]

        for index, column in enumerate(self.columns):
            for transformer in self.columnTransforms.get(column, []):
                for row in rows:
                    value = row[index]
                    try:
                        row[index] = transformer(value)
                    except Exception as error:
                        transformerName = getattr(transformer, '__name__', repr(transformer))
                        raise TransformError('transformer "{}" failed on column "{}" for value {!r}: {}'.format(transformerName, column, value, error)) from error

        return [tuple(row) for row in rows]
