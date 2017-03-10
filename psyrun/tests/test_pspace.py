import numpy as np
import pytest

from psyrun.pspace import AmbiguousOperationError, dict_concat, missing, Param


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

    def test_treats_string_not_as_sequence(self):
        assert Param(a='item').build()['a'] == ['item']
        assert Param(a=['item']).build()['a'] == ['item']

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

    def test_from_dict(self):
        d = {'a': [1, 2], 'b.x': 3, 'b.y': [4, 5]}
        space = Param(**d).build()
        assert space['a'] == [1, 2]
        assert space['b.x'] == [3, 3]
        assert space['b.y'] == [4, 5]

    def test_hierarchical(self):
        d1 = {'root.a': [1, 2], 'b': [3, 4]}
        d2 = {'root.b': [5, 6]}
        space = (Param(**d1) * Param(**d2)).build()
        assert space['root.a'] == [1, 1, 2, 2]
        assert space['b'] == [3, 3, 4, 4]
        assert space['root.b'] == [5, 6, 5, 6]

    def test_str(self):
        p = Param(**{'a': [1, 2], 'b.b': [3, 4]})
        assert str(p) == "Param(a=[1, 2], b.b=[3, 4])"

    def test_rep(self):
        p = Param(**{'a': [1, 2], 'b.b': [3, 4]})
        assert repr(p) == "Param(**{'a': [1, 2], 'b.b': [3, 4]})"


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

    def test_multidimensional(self):
        space = (Param(a=[[1, 1], [2, 2]]) * Param(b=[1, 2])).build()
        assert space['a'] == [[1, 1], [1, 1], [2, 2], [2, 2]]
        assert space['b'] == [1, 2, 1, 2]


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

    def test_multidimensional(self):
        space = (Param(a=[[1, 1], [2, 2]]) + Param(a=[[3, 3]])).build()
        assert space['a'] == [[1, 1], [2, 2], [3, 3]]

        space = (Param(a=[[1, 1], [2, 2]]) + Param(a=[3, 4])).build()
        assert space['a'] == [[1, 1], [2, 2], 3, 4]


class TestDifference(object):
    def test_difference(self):
        space = (Param(a=[1, 2, 3]) - Param(a=[2])).build()
        assert sorted(space['a']) == [1, 3]

    def test_multiple_builds(self):
        space = (Param(a=[1, 2, 3]) - Param(a=[2]))
        assert sorted(space.build()['a']) == [1, 3]
        assert sorted(space.build()['a']) == [1, 3]

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

    def test_multidimensional(self):
        space = (Param(a=[[1, 1], [2, 2]]) - Param(a=[[2, 2]])).build()
        assert space['a'] == [[1, 1]]


class TestMissing(object):
    def test_missing(self):
        space = missing(Param(a=[1, 2, 3]), Param(a=[2])).build()
        assert sorted(space['a']) == [1, 3]

    def test_param_missing_in_subtrahend(self):
        with pytest.raises(AmbiguousOperationError):
            missing(Param(a=[1, 2, 2], b=[4, 5, 6]), Param(a=[2])).build()

    def test_param_missign_in_minuend(self):
        space = missing(Param(a=[1, 2, 3]), Param(a=[2], b=[3])).build()
        assert sorted(space['a']) == [1, 3]

    def test_empty_subtrahend(self):
        space = missing(Param(a=[1, 2, 3]), Param()).build()
        assert sorted(space['a']) == [1, 2, 3]

    def test_length(self):
        assert len(missing(Param(a=[1, 2, 3]), Param(a=[2]))) == 2

    def test_multidimensional(self):
        space = missing(Param(a=[[1, 1], [2, 2]]), Param(a=[[1, 1]])).build()
        assert space['a'] == [[2, 2]]
