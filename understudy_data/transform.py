from __future__ import annotations

import ast
import importlib
import inspect
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

Transformer = Callable[[Any], Any]


class TransformResolutionError(Exception):
    """Raised when a "module.path:function_name" transformer reference can't be resolved."""


class TransformError(Exception):
    """A transform names a column the source query doesn't return, or a
    transformer raised on a value.

    Names the column and the value's type, never the value: transforms see
    unmasked production rows. The original exception isn't chained, since it
    may quote the value.
    """


# function_name, optionally followed by an argument list: truncate(50)
_CALL = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\((.*)\))?\s*$', re.DOTALL)


def _parseArguments(reference: str, text: str) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    """The literal arguments in `truncate(50, suffix='...')`. Only literals,
    so configuration can't run code this way.
    """

    try:
        call = ast.parse('f({})'.format(text), mode='eval').body
        if not isinstance(call, ast.Call):
            raise ValueError('not a single argument list')
        arguments = tuple(ast.literal_eval(node) for node in call.args)
        keywords = {keyword.arg: ast.literal_eval(keyword.value) for keyword in call.keywords if keyword.arg is not None}
        if len(keywords) != len(call.keywords) or any(isinstance(node, ast.Starred) for node in call.args):
            raise ValueError('unpacking is not allowed')
    except (SyntaxError, ValueError) as error:
        raise TransformResolutionError(f'transformer reference "{reference}": arguments must be literal values ({error})') from None

    return arguments, keywords


def _checkSignature(function: Transformer, reference: str, arguments: Tuple[Any, ...], keywords: Dict[str, Any]) -> None:
    """Fails now, so `validate` catches it, if the column value plus these
    arguments can't be passed -- rather than on the first row of a run.
    """

    try:
        signature = inspect.signature(function)
    except (TypeError, ValueError):
        return  # a builtin without an inspectable signature; the call itself will say

    try:
        signature.bind(None, *arguments, **keywords)
    except TypeError as error:
        raise TransformResolutionError(f'transformer reference "{reference}": {error}') from None


def _withArguments(function: Transformer, reference: str, arguments: Tuple[Any, ...], keywords: Dict[str, Any]) -> Transformer:
    """`function` with its extra arguments fixed, still taking one value."""

    def transformer(value: Any) -> Any:
        return function(value, *arguments, **keywords)

    transformer.__name__ = reference.partition(':')[2].strip()

    return transformer


def resolveTransformer(reference: str) -> Transformer:
    """Import a Transformer from a "module.path:function_name" reference.
    Arguments after the value go in parentheses, as literals:
    "understudy_data.builtinTransforms:truncate(50)" calls truncate(value, 50).
    """

    modulePath, separator, call = reference.partition(':')

    if not separator:
        raise TransformResolutionError(f'transformer reference "{reference}" must be in the form "module.path:function_name"')

    match = _CALL.match(call)
    if match is None:
        raise TransformResolutionError(f'transformer reference "{reference}": "{call}" is not a function name, with or without (arguments)')
    attributeName, argumentText = match.group(1), match.group(2)

    try:
        module = importlib.import_module(modulePath)
    except ImportError as error:
        raise TransformResolutionError(f'transformer reference "{reference}": no module named "{modulePath}"') from error

    transformer = getattr(module, attributeName, None)

    if transformer is None:
        raise TransformResolutionError(f'transformer reference "{reference}": "{modulePath}" has no attribute "{attributeName}"')
    if not callable(transformer):
        raise TransformResolutionError(f'transformer reference "{reference}": "{attributeName}" is not callable')

    arguments, keywords = _parseArguments(reference, argumentText) if argumentText is not None else ((), {})
    _checkSignature(transformer, reference, arguments, keywords)

    if argumentText is None:
        return transformer

    return _withArguments(transformer, reference, arguments, keywords)


class Transform:
    """Applies per-column transformers to rows: validate() once, then apply()
    per chunk. transform() does both for a whole `data`.
    """

    def __init__(self, columns: List[str], columnTransforms: Dict[str, List[Transformer]],
                  data: Optional[List[Tuple[Any, ...]]] = None) -> None:
        self.columns = columns
        self.columnTransforms = columnTransforms
        self.data = data if data is not None else []
        self._transformsByIndex = [(index, self.columnTransforms.get(column, [])) for index, column in enumerate(self.columns)]


    def validate(self) -> None:
        """Raises TransformError if any transform names a column that isn't
        there. Run before the first write.
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
                        raise TransformError('transformer "{}" failed on column "{}" for a value of type {}: {}'.format(
                            transformerName, self.columns[index], type(value).__name__, type(error).__name__)) from None

        return [tuple(row) for row in rows]


    def transform(self) -> List[Tuple[Any, ...]]:
        """Validate and transform the whole of `data` in one call."""

        self.validate()

        return self.apply(self.data)
