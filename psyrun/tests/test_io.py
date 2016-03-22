import pytest

import numpy as np

from psyrun.io import H5Store, NpzStore
from psyrun.pspace import Param


@pytest.mark.parametrize(
    'store,ext', [(H5Store(), '.h5'), (NpzStore(), '.npz')])
class TestDictStores(object):
    def test_infile_with_none(self, store, ext, tmpdir):
        filename = str(tmpdir.join('infile' + ext))
        pspace = (Param(a=[1.], b=[1.]) + Param(a=[2.], c=[2.])).build()
        store.save(filename, pspace)
        saved_pspace = store.load(filename)
        assert sorted(pspace.keys()) == sorted(saved_pspace.keys())
        for k in pspace:
            for a, b in zip(pspace[k], saved_pspace[k]):
                assert a == b or (np.isnan(a)and np.isnan(b))


    def test_merging_multidimensional_results(self, store, ext, tmpdir):
        filename = str(tmpdir.join('r' + ext))

        data1 = {'a': np.zeros((2, 2, 2))}
        data2 = {'a': np.ones((3, 2, 2))}

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        assert np.all(np.concatenate([data1['a'], data2['a']]) == saved['a'])


    def test_merging_results_with_varying_dimensionality(
            self, store, ext, tmpdir):
        filename = str(tmpdir.join('r' + ext))

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
