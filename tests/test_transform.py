import pytest

from lightweight_etl.transform import Transform, TransformError, TransformResolutionError, resolveTransformer


def test_transform_applies_an_arbitrary_callable():
    rows = [(1, 100.5), (2, None)]

    transform = Transform(data=rows, columns=['id', 'amount'], columnTransforms={'amount': [lambda v: '${:,.2f}'.format(v or 0)]})

    assert transform.transform() == [(1, '$100.50'), (2, '$0.00')]


def test_transform_applies_multiple_transformers_in_order():
    rows = [(1,)]

    transform = Transform(data=rows, columns=['n'], columnTransforms={'n': [lambda v: v + 1, lambda v: v * 10]})

    assert transform.transform() == [(20,)]


def test_transform_with_no_data_returns_it_unchanged():
    transform = Transform(data=[], columns=['id'], columnTransforms={'id': [lambda v: v + 1]})

    assert transform.transform() == []


def test_transform_leaves_untransformed_columns_alone():
    rows = [(1, 'a')]

    transform = Transform(data=rows, columns=['id', 'name'], columnTransforms={})

    assert transform.transform() == [(1, 'a')]


def test_transform_raises_when_a_transform_names_a_column_not_in_columns():
    """A column not in `columns` -- e.g. left out of targetColumns, or never
    actually selected -- should fail loudly and up front, rather than the
    transform silently never running.
    """
    transform = Transform(data=[(1,)], columns=['id'], columnTransforms={'doesNotExist': [lambda v: v]})

    with pytest.raises(TransformError, match='doesNotExist'):
        transform.transform()


def test_transform_raises_when_a_transform_names_a_column_not_in_columns_even_with_no_rows():
    """The unknown-column check runs before the (data-empty) early return, so a
    misconfigured job fails the same way regardless of how much data it moves.
    """
    transform = Transform(data=[], columns=['id'], columnTransforms={'doesNotExist': [lambda v: v]})

    with pytest.raises(TransformError, match='doesNotExist'):
        transform.transform()


def test_transform_wraps_a_failing_transformer_with_column_and_type_context():
    def onlyAcceptsStrings(value):
        return value.upper()

    transform = Transform(data=[(1,)], columns=['id'], columnTransforms={'id': [onlyAcceptsStrings]})

    with pytest.raises(TransformError, match='column "id"') as excinfo:
        transform.transform()

    assert 'onlyAcceptsStrings' in str(excinfo.value)
    assert 'of type int' in str(excinfo.value)


def test_a_failing_transformer_never_reveals_the_value():
    """Transforms see raw production rows, before masking, and the error text
    reaches logs and the run result. The transformer's own message is dropped
    too, since it usually quotes the value.
    """
    def parseNumber(value):
        return int(value)

    transform = Transform(data=[('123-45-6789',)], columns=['ssn'], columnTransforms={'ssn': [parseNumber]})

    with pytest.raises(TransformError) as excinfo:
        transform.transform()

    assert '123-45-6789' not in str(excinfo.value)
    assert excinfo.value.__cause__ is None
    assert excinfo.value.__suppress_context__


def test_resolve_transformer_finds_a_real_function():
    """lightweight_etl itself ships no transforms module (that lives in example/, since
    it's reference content, not library code) -- resolve a stdlib function instead
    to test the resolution mechanics without depending on example/'s existence.
    """
    transformer = resolveTransformer('os.path:basename')

    assert transformer('/a/b/c.txt') == 'c.txt'


def test_resolve_transformer_requires_a_colon():
    with pytest.raises(TransformResolutionError):
        resolveTransformer('os.path.basename')


def test_resolve_transformer_raises_for_missing_module():
    with pytest.raises(TransformResolutionError):
        resolveTransformer('not_a_real_module_xyz:fn')


def test_resolve_transformer_raises_for_missing_attribute():
    with pytest.raises(TransformResolutionError):
        resolveTransformer('os.path:doesNotExist')


def test_resolve_transformer_raises_for_non_callable_attribute():
    with pytest.raises(TransformResolutionError):
        resolveTransformer('os.path:sep')
