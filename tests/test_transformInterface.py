import pytest

from library.transformInterface import Transform, TransformResolutionError, resolveTransformer


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


def test_resolve_transformer_finds_a_real_function():
    """library itself ships no transforms module (that lives in example/, since
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
