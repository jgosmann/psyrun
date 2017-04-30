import pytest

import numpy as np
from numpy.testing import assert_array_equal

from psyrun.store import AutodetectStore
from psyrun.store.h5 import H5Store
from psyrun.store.npz import NpzStore
from psyrun.store.pickle import PickleStore
from psyrun.pspace import Param


@pytest.mark.parametrize('store', [H5Store(), NpzStore(), PickleStore()])
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
        assert np.all(
            np.concatenate([data1['a'], data2['a']]) == np.asarray(saved['a']))

    def test_merging_results_with_varying_dimensionality(self, store, tmpdir):
        filename = str(tmpdir.join('r' + store.ext))

        data1 = {'a': np.zeros((1, 1, 2))}
        data2 = {'a': np.ones((2, 2, 1))}

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        assert np.all(saved['a'][0][:1, :2] == data1['a'][0])
        assert np.all(saved['a'][1][:2, :1] == data2['a'][0])
        assert np.all(saved['a'][2][:2, :1] == data2['a'][1])


@pytest.mark.parametrize('store', [NpzStore(), PickleStore()])
class TestStringStoring(object):
    def test_saves_single_string_list(self, store, tmpdir):
        filename = str(tmpdir.join('r' + store.ext))

        data1 = {'a': ['A', 'B'], 'num': [1, 2]}
        data2 = {'a': ['AB'], 'num': [3]}

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        assert np.all(np.concatenate([data1['a'], data2['a']]) == saved['a'])
        assert np.all(np.array([1, 2, 3]) == saved['num'])

    def test_handles_missing_values(self, store, tmpdir):
        filename = str(tmpdir.join('r' + store.ext))

        data1 = {'num': [1, 2], 'a': ['A', 'B']}
        data2 = {'num': [3], 'b': ['AB']}

        store.append(filename, data1)
        store.append(filename, data2)

        saved = store.load(filename)
        print(saved)
        assert np.all(saved['a'][:2] == data1['a'])
        assert np.all(saved['b'][2:] == data2['b'])


@pytest.mark.parametrize('store', [H5Store()])
def test_non_string_supporting(tmpdir, store):
    filename = str(tmpdir.join('r' + store.ext))

    with pytest.raises(NotImplementedError):
        store.save(filename, {'a': ['some string']})

    with pytest.raises(NotImplementedError):
        store.append(filename, {'a': ['some string']})


@pytest.mark.parametrize('store', [NpzStore(), PickleStore()])
def test_dict_and_lists(store, tmpdir):
    filename = str(tmpdir.join('r' + store.ext))

    obj1 = {'x': [1, 2]}
    obj2 = {'y': [3, 4]}

    store.append(filename, {'a': [obj1]})
    store.append(filename, {'a': [obj2]})

    saved = store.load(filename)
    print(store, saved)
    assert np.all(saved['a'] == [obj1, obj2])


class ObjectMock(object):
    def __init__(self, value):
        self.value = value


@pytest.mark.parametrize('store', [NpzStore(), PickleStore()])
def test_object(store, tmpdir):
    filename = str(tmpdir.join('r' + store.ext))

    obj1 = ObjectMock(1)
    obj2 = ObjectMock(2)

    store.append(filename, {'a': [obj1]})
    store.append(filename, {'a': [obj2]})

    saved = store.load(filename)
    print(store, saved)
    assert saved['a'][0].value == 1
    assert saved['a'][1].value == 2


@pytest.mark.parametrize('store', [NpzStore(), PickleStore(), H5Store()])
def test_autodetect_store_load(store, tmpdir):
    filename = str(tmpdir.join('r' + store.ext))
    store.save(filename, {'a': [1., 2., 3.]})

    data = AutodetectStore().load(filename)
    assert list(data.keys()) == ['a']
    assert_array_equal(data['a'], [1., 2., 3.])


@pytest.mark.parametrize('store', [NpzStore(), PickleStore(), H5Store()])
def test_autodetect_store_save(store, tmpdir):
    filename = str(tmpdir.join('r' + store.ext))
    AutodetectStore().save(filename, {'a': [1., 2., 3.]})

    data = store.load(filename)
    assert list(data.keys()) == ['a']
    assert_array_equal(data['a'], [1., 2., 3.])


@pytest.mark.parametrize('store', [NpzStore(), PickleStore(), H5Store()])
def test_autodetect_store_append(store, tmpdir):
    filename = str(tmpdir.join('r' + store.ext))
    store.append(filename, {'a': [1.]})
    AutodetectStore().append(filename, {'a': [2., 3.]})

    data = store.load(filename)
    assert list(data.keys()) == ['a']
    assert_array_equal(data['a'], [1., 2., 3.])
