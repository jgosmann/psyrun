import pytest

import numpy as np

from psyrun.store import H5Store, NpzStore
from psyrun.pspace import Param


@pytest.mark.parametrize('store', [H5Store(), NpzStore()])
class TestDictStores(object):
    def test_infile_with_none(self, store, tmpdir):
        filename = str(tmpdir.join('infile' + store.ext))
        pspace = (Param(a=[1.], b=[1.]) + Param(a=[2.], c=[2.])).build()
        store.save(filename, pspace)
        saved_pspace = store.load(filename)
        assert sorted(pspace.keys()) == sorted(saved_pspace.keys())
        for k in pspace:
            for a, b in zip(pspace[k], saved_pspace[k]):
                assert a == b or (np.isnan(a)and np.isnan(b))

    def test_load_specific_row(self, store, tmpdir):
        filename = str(tmpdir.join('infile' + store.ext))
        pspace = Param(a=range(5)).build()
        store.save(filename, pspace)
        saved_pspace = store.load(filename, row=2)
        assert sorted(pspace.keys()) == sorted(saved_pspace.keys())
        assert list(saved_pspace['a']) == [2]

    def test_merging_multidimensional_results(self, store, tmpdir):
        filename = str(tmpdir.join('r' + store.ext))

        data1 = {'a': np.zeros((2, 2, 2))}
        data2 = {'a': np.ones((3, 2, 2))}

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        assert np.all(np.concatenate([data1['a'], data2['a']]) == saved['a'])

    def test_merging_results_with_varying_dimensionality(self, store, tmpdir):
        filename = str(tmpdir.join('r' + store.ext))

        data1 = {'a': np.zeros((1, 1, 2))}
        data2 = {'a': np.ones((2, 2, 1))}
        expected = np.array([
            [[0., 0.], [np.nan, np.nan]],
            [[1., np.nan], [1., np.nan]],
            [[1., np.nan], [1., np.nan]]])

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        assert expected.shape == saved['a'].shape
        for a, b in zip(expected.flat, saved['a'].flat):
            assert a == b or (np.isnan(a) and np.isnan(b))


@pytest.mark.parametrize('store', [NpzStore()])
class TestStringStoring(object):
    def test_saves_single_strings(self, store, tmpdir):
        filename = str(tmpdir.join('r' + store.ext))

        data1 = {'a': 'A', 'num': [1]}
        data2 = {'a': 'AB', 'num': [2]}

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        assert np.all(np.array([data1['a'], data2['a']]) == saved['a'])
        assert np.all(np.array([1, 2]) == saved['num'])

    def test_saves_single_string_list(self, store, tmpdir):
        filename = str(tmpdir.join('r' + store.ext))

        data1 = {'a': ['A', 'B'], 'num': [1, 2]}
        data2 = {'a': ['AB'], 'num': [3]}

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        assert np.all(np.concatenate([data1['a'], data2['a']]) == saved['a'])
        assert np.all(np.array([1, 2, 3]) == saved['num'])


@pytest.mark.parametrize('store', [H5Store()])
def test_non_string_supporting(tmpdir, store):
    filename = str(tmpdir.join('r' + store.ext))

    with pytest.raises(NotImplementedError):
        store.save(filename, {'a': ['some string']})

    with pytest.raises(NotImplementedError):
        store.append(filename, {'a': ['some string']})
