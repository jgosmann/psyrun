import numpy as np
import pytest

from psyrun.pspace import AmbiguousOperationError, dict_concat, Param


@pytest.mark.parametrize('args,result', [
    (({0: 0}, {0: 1}, {0: 2}), {0: [0, 1, 2]}),
    (({0: 2},), {0: [2]}),
    (({},), {}),
    (({0: 0, 1: 1}, {0: 0, 1: 1}), {0: [0, 0], 1: [1, 1]}),
    ((), {}),
    (({0: 0}, {1: 1}), {0: [0, None], 1: [None, 1]})
])
def test_dict_concat(args, result):
    assert dict_concat(args) == result


class TestParam(object):
    def test_empty(self):
        assert Param().build() == {}

    def test_single(self):
        space = Param(a=[1, 2]).build()
        assert sorted(space['a']) == [1, 2]

    def test_multiple(self):
        space = Param(a=[1, 2], b=[3, 4]).build()
        assert sorted(space['a']) == [1, 2]
        assert sorted(space['b']) == [3, 4]

    def test_multiple_unequal_length_raises_error(self):
        with pytest.raises(ValueError):
            Param(a=[1, 2], b=[3])

    def test_only_scalars(self):
        space = Param(a=1, b=2).build()
        assert space['a'] == [1]
        assert space['b'] == [2]

    def test_scalars_are_broadcasted(self):
        space = Param(a=[1, 2], b=3).build()
        assert sorted(space['a']) == [1, 2]
        assert sorted(space['b']) == [3, 3]

    def test_knows_about_its_keys(self):
        assert sorted(Param(a=[0], b=[1]).keys()) == ['a', 'b']

    def test_length(self):
        assert len(Param(a=[1, 2, 3], b=3)) == 3

    def test_keeps_dtype(self):
        space = Param(a=[1], b=[2.]).build()
        assert np.issubdtype(type(space['a'][0]), int)
        assert np.issubdtype(type(space['b'][0]), float)


class TestProduct(object):
    def test_product(self):
        space = (Param(a=[1, 2, 3]) * Param(b=[4, 5])).build()
        assert space['a'] == [1, 1, 2, 2, 3, 3]
        assert space['b'] == [4, 5, 4, 5, 4, 5]

    def test_overlapping_product(self):
        with pytest.raises(AmbiguousOperationError):
            _ = Param(a=[1, 2]) * Param(a=[2, 3], b=[4, 5])

    def test_product_with_empty_right(self):
        space = (Param(a=[1, 2, 3]) * Param()).build()
        assert sorted(space['a']) == [1, 2, 3]

    def test_product_with_empty_left(self):
        space = (Param() * Param(a=[1, 2, 3])).build()
        assert sorted(space['a']) == [1, 2, 3]

    def test_product_with_zero_elements(self):
        space = (Param(a=[1, 2, 3]) * Param(b=[])).build()
        assert space['a'] == []
        assert space['b'] == []

    def test_length(self):
        assert len(Param(a=[1, 2, 3]) * Param(b=[1, 2])) == 6
        assert len(Param(a=[1, 2]) * Param()) == 2
        assert len(Param() * Param(a=[1, 2])) == 2
        assert len(Param(a=[1, 2]) * Param(b=[])) == 0
        assert len(Param(a=[]) * Param(b=[1, 2])) == 0


class TestSum(object):
    def test_sum(self):
        space = (Param(a=[1]) + Param(a=[2])).build()
        assert sorted(space['a']) == [1, 2]

    def test_sum_distinct_params(self):
        space = (Param(a=[1]) + Param(b=[2])).build()
        assert space['a'][0] == 1
        assert space['b'][1] == 2
        assert np.isnan(space['a'][1])
        assert np.isnan(space['b'][0])

    def test_sum_with_empty(self):
        space = (Param(a=[1]) + Param()).build()
        assert sorted(space['a']) == [1]

    def test_sum_with_zero_elements(self):
        space = (Param(a=[1]) + Param(b=[])).build()
        assert space['a'] == [1]
        assert np.isnan(space['b'][0])

    def test_length(self):
        assert len(Param(a=[1]) + Param(a=[2, 3])) == 3


class TestDifference(object):
    def test_difference(self):
        space = (Param(a=[1, 2, 3]) - Param(a=[2])).build()
        assert sorted(space['a']) == [1, 3]

    def test_param_missing_in_subtrahend(self):
        space = (Param(a=[1, 2, 2], b=[4, 5, 6]) - Param(a=[2])).build()
        assert sorted(space['a']) == [1]
        assert sorted(space['b']) == [4]

    def test_param_missign_in_minuend(self):
        with pytest.raises(AmbiguousOperationError):
            _ = Param(a=[1, 2, 3]) - Param(a=[2], b=[3])

    def test_empty_subtrahend(self):
        space = (Param(a=[1, 2, 3]) - Param()).build()
        assert sorted(space['a']) == [1, 2, 3]

    def test_length(self):
        assert len(Param(a=[1, 2, 2], b=[4, 5, 6]) - Param(a=[2])) == 1
