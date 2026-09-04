from __future__ import annotations

import importlib
from typing import Any, Callable, Dict, List, Tuple

Transformer = Callable[[Any], Any]


class TransformResolutionError(Exception):
    """Raised when a "module.path:function_name" transformer reference can't be resolved."""


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
        if not len(self.data):
            return self.data

        rows = [list(row) for row in self.data]

        for index, column in enumerate(self.columns):
            for transformer in self.columnTransforms.get(column, []):
                for row in rows:
                    row[index] = transformer(row[index])

        return [tuple(row) for row in rows]
