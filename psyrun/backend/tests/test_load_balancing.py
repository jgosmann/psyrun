import pytest

from psyrun.backend.load_balancing import LoadBalancingWorker
from psyrun.pspace import Param
from psyrun.store.h5 import H5Store
from psyrun.store.npz import NpzStore
from psyrun.store.pickle import PickleStore


def square(a):
    return {'x': a ** 2}


@pytest.mark.parametrize('store', [PickleStore(), NpzStore(), H5Store()])
def test_load_balancing_worker(tmpdir, store):
    infile = str(tmpdir.join('in.npz'))
    outfile = str(tmpdir.join('out.npz'))
    statusfile = str(tmpdir.join('status'))
    store.save(infile, Param(a=range(7)).build())
    LoadBalancingWorker.create_statusfile(statusfile)
    worker = LoadBalancingWorker(infile, outfile, statusfile, store)
    worker.start(square)
    result = store.load(outfile)
    assert sorted(result['a']) == sorted(range(7))
    assert sorted(result['x']) == [i ** 2 for i in range(7)]
