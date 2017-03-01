import os.path

import pytest

from psyrun.store.h5 import H5Store
from psyrun.store.npz import NpzStore
from psyrun.store.pickle import PickleStore
from psyrun.pspace import Param
from psyrun.mapper import (
    map_pspace, map_pspace_parallel, map_pspace_hdd_backed)


def square(a):
    return {'x': a ** 2}


def test_map_pspace():
    calls = []

    def fn(**kwargs):
        calls.append(kwargs)
        return {'result': 42}

    pspace = Param(a=[1, 2])
    result = map_pspace(fn, pspace)

    assert calls == [{'a': 1}, {'a': 2}]
    assert result == {'a': [1, 2], 'result': [42, 42]}


@pytest.mark.parametrize('store', [PickleStore(), H5Store(), NpzStore()])
def test_hdd_backed_mapper(tmpdir, store):
    pspace = Param(a=[1, 2])
    filename = os.path.join(str(tmpdir), 'out' + store.ext)
    result = map_pspace_hdd_backed(
        square, pspace, filename=filename, store=store)
    assert list(result['a']) == [1, 2]
    assert list(result['x']) == [1, 4]
    loaded = store.load(filename)
    assert list(loaded['a']) == [1, 2]
    assert list(loaded['x']) == [1, 4]


@pytest.mark.parametrize('store', [PickleStore(), H5Store(), NpzStore()])
def test_hdd_backed_mapper_continues(tmpdir, store):
    pspace = Param(a=[1, 2])
    filename = os.path.join(str(tmpdir), 'out' + store.ext)
    store.append(filename, {'a': [1], 'x': [-1]})
    result = map_pspace_hdd_backed(
        square, pspace, filename=filename, store=store)
    assert list(result['a']) == [1, 2]
    assert list(result['x']) == [-1, 4]
    loaded = store.load(filename)
    assert list(loaded['a']) == [1, 2]
    assert list(loaded['x']) == [-1, 4]


def test_map_pspace_parallel():
    pspace = Param(a=[1, 2])
    result = map_pspace_parallel(square, pspace)
    assert result == {'a': [1, 2], 'x': [1, 4]}
