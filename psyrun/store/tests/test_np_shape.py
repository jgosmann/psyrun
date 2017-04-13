import numpy as np
from numpy.testing import assert_equal
import pytest

from psyrun.store.np_shape import min_shape, match_shape


class Obj(object):
    def __init__(self, value):
        self.value = value

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return isinstance(other, Obj) and self.value == other.value


def test_min_shape():
    assert min_shape(((1, 1, 1), (2, 3, 4))) == (2, 3, 4)
    assert min_shape(((3, 2, 1), (1, 2, 3))) == (3, 2, 3)
    assert min_shape(((1, 1, 2), (1, 2, 1), (2, 1, 1))) == (2, 2, 2)
    assert min_shape(((1,), (2,))) == (2,)
    assert min_shape((tuple(), tuple())) == tuple()
    with pytest.raises(ValueError):
        assert min_shape(((1, 3), (2, 1, 3)))


@pytest.mark.parametrize('a,shape,expected', [
    ([[1.], [2.]], (2,), [[1., np.nan], [2., np.nan]]),
    ([[1j], [2j]], (2,), [[1j, np.nan], [2j, np.nan]]),
    ([[1], [2]], (2,), [[1., np.nan], [2., np.nan]]),
    ([['a'], ['b']], (2,), [['a', ''], ['b', '']]),
    ([[Obj(1)], [Obj(2)]], (2,), [[Obj(1), None], [Obj(2), None]]),
    (1., (1,), [[1.]]),
])
def test_match_shape(a, shape, expected):
    assert_equal(match_shape(a, shape), np.asarray(expected))


def test_match_shape_with_unsupported_missing():
    with pytest.raises(ValueError):
        match_shape(np.ones((2, 1), dtype='M'), (2,))
