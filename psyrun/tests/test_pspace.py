import itertools

import pandas as pd
import pytest

from psyrun.pspace import AmbiguousOperationError, Param


class TestParam(object):
    def test_empty(self):
        assert Param().build().empty

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

    def test_only_scalars_raises_error(self):
        with pytest.raises(ValueError):
            Param(a=1, b=2)

    def test_scalars_are_broadcasted(self):
        space = Param(a=[1, 2], b=3).build()
        assert sorted(space['a']) == [1, 2]
        assert sorted(space['b']) == [3, 3]

    def test_knows_about_its_keys(self):
        assert sorted(Param(a=[0], b=[1]).keys()) == ['a', 'b']

    def test_length(self):
        assert len(Param(a=[1, 2, 3], b=3)) == 3


class TestProduct(object):
    def test_product(self):
        space = (Param(a=[1, 2, 3]) * Param(b=[4, 5])).build()
        for a, b in itertools.product([1, 2, 3], [4, 5]):
            assert (space == pd.Series({'a': a, 'b': b})).all(1).any()

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
        assert space.empty

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
        nan_placeholder = 42
        space = (Param(a=[1]) + Param(b=[2])).build().fillna(nan_placeholder)
        assert sorted(space['a']) == sorted([1, nan_placeholder])
        assert sorted(space['b']) == sorted([2, nan_placeholder])

    def test_sum_with_empty(self):
        space = (Param(a=[1]) + Param()).build()
        assert sorted(space['a']) == [1]

    def test_sum_with_zero_elements(self):
        nan_placeholder = 42
        space = (Param(a=[1]) + Param(b=[])).build().fillna(nan_placeholder)
        assert sorted(space['a']) == sorted([1])
        assert sorted(space['b']) == sorted([nan_placeholder])

    def test_legth(self):
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
