import numpy as np

from psyrun.io import append_dict_h5, load_dict_h5, save_dict_h5
from psyrun.pspace import Param


def test_infile_with_none(tmpdir):
    filename = str(tmpdir.join('infile.h5'))
    pspace = (Param(a=[1.], b=[1.]) + Param(a=[2.], c=[2.])).build()
    save_dict_h5(filename, pspace)
    saved_pspace = load_dict_h5(filename)
    assert sorted(pspace.keys()) == sorted(saved_pspace.keys())
    for k in pspace:
        for a, b in zip(pspace[k], saved_pspace[k]):
            assert a == b or (np.isnan(a)and np.isnan(b))


def test_merging_multidimensional_results(tmpdir):
    filename = str(tmpdir.join('r.h5'))

    data1 = {'a': np.zeros((2, 2, 2))}
    data2 = {'a': np.ones((3, 2, 2))}

    append_dict_h5(filename, data1)
    append_dict_h5(filename, data2)

    saved = load_dict_h5(filename)
    assert np.all(np.concatenate([data1['a'], data2['a']]) == saved['a'])


def test_merging_results_with_varying_dimensionality(tmpdir):
    filename = str(tmpdir.join('r.h5'))

    data1 = {'a': np.zeros((1, 1, 2))}
    data2 = {'a': np.ones((2, 2, 1))}
    expected = np.array([
        [[0., 0.], [np.nan, np.nan]],
        [[1., np.nan], [1., np.nan]],
        [[1., np.nan], [1., np.nan]]])

    append_dict_h5(filename, data1)
    append_dict_h5(filename, data2)

    saved = load_dict_h5(filename)
    assert expected.shape == saved['a'].shape
    for a, b in zip(expected.flat, saved['a'].flat):
        assert a == b or (np.isnan(a) and np.isnan(b))
