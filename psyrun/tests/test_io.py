import numpy as np

from psyrun.io import append_to_results, load_infile, load_results, save_infile
from psyrun.pspace import Param


def test_infile_with_none(tmpdir):
    filename = str(tmpdir.join('infile.h5'))
    pspace = (Param(a=[1.], b=[1.]) + Param(a=[2.], c=[2.])).build()
    save_infile(pspace, filename)
    saved_pspace = load_infile(filename).build()
    assert sorted(pspace.keys()) == sorted(saved_pspace.keys())
    for k in pspace:
        for a, b in zip(pspace[k], saved_pspace[k]):
            assert a == b or (np.isnan(a)and np.isnan(b))


def test_merging_multidimensional_results(tmpdir):
    filename = str(tmpdir.join('r.h5'))

    data1 = {'a': np.zeros((2, 2, 2))}
    data2 = {'a': np.ones((3, 2, 2))}

    append_to_results(data1, filename)
    append_to_results(data2, filename)

    saved = load_results(filename)
    assert np.all(np.concatenate([data1['a'], data2['a']]) == saved['a'])


def test_merging_results_with_varying_dimensionality(tmpdir):
    filename = str(tmpdir.join('r.h5'))

    data1 = {'a': np.zeros((1, 1, 2))}
    data2 = {'a': np.ones((2, 2, 1))}
    expected = np.array([
        [[0., 0.], [np.nan, np.nan]],
        [[1., np.nan], [1., np.nan]],
        [[1., np.nan], [1., np.nan]]])

    append_to_results(data1, filename)
    append_to_results(data2, filename)

    saved = load_results(filename)
    assert expected.shape == saved['a'].shape
    for a, b in zip(expected.flat, saved['a'].flat):
        assert a == b or (np.isnan(a) and np.isnan(b))
