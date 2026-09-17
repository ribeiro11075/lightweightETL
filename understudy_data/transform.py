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
    """Raised when applying a resolved transformer actually fails.

    Two distinct cases surface here, both eagerly rather than as a bare traceback
    deep inside a row loop: (1) columnTransforms names a column that isn't in
    `columns` at all -- for understudy's own job-running path (see
    runner.py's _executeDataJob), `columns` there is sourceQuery's own result
    columns (from cursor.description -- whatever it actually selected, explicit
    list or `select *` alike), not the target table, since a transform runs on a
    value as extracted from the source, before it's mapped onto any target column
    name -- checked up front, before touching any row, so it fails the same way
    every time instead of being silently skipped; (2) a transformer raises on a
    particular value (e.g. a `str`-only transform handed a row where that column
    is an int) -- caught per-value and re-raised with the column name and the
    value's *type* attached, since the original exception alone doesn't say
    which column caused it.

    Never the value itself. Transforms run on raw production rows, before
    masking, and this message ends up in logs, the run result and the job's
    outcome -- one bad value would otherwise copy personal data into all three.
    The original exception isn't chained for the same reason: drivers and
    transformers routinely put the value in their own message.
    """


# function_name, optionally followed by an argument list: truncate(50)
_CALL = re.compile(r'^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\((.*)\))?\s*$', re.DOTALL)


def _parseArguments(reference: str, text: str) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    """The literal arguments in `truncate(50, suffix='...')`.

    Parsed as Python syntax, but only literals are accepted -- numbers,
    strings, True, False, None, and tuples or lists of those -- so a
    configuration file can't run code this way.
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

    Lets a job configuration name a function defined anywhere importable --
    understudy_data/builtinTransforms.py, or any module of the user's own -- without the caller
    having to pre-register it in a lookup table.

    Arguments after the column value go in parentheses, as literals:
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
                        raise TransformError('transformer "{}" failed on column "{}" for a value of type {}: {}'.format(
                            transformerName, self.columns[index], type(value).__name__, type(error).__name__)) from None

        return [tuple(row) for row in rows]


    def transform(self) -> List[Tuple[Any, ...]]:
        """Validate and transform the whole of `data` in one call."""

        self.validate()

        return self.apply(self.data)
