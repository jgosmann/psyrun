import os.path

import pytest

from psyrun.io import load_results, save_infile
from psyrun.pspace import Param
from psyrun.worker import map_pspace, ParallelWorker, SerialWorker


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


@pytest.mark.parametrize('worker', [SerialWorker(), ParallelWorker()])
def test_worker(worker, tmpdir):
    infile = os.path.join(str(tmpdir), 'in.h5')
    outfile = os.path.join(str(tmpdir), 'out.h5')
    save_infile(Param(a=range(7)).build(), infile)
    worker.start(square, infile, outfile)
    result = load_results(outfile)
    assert sorted(result['a']) == sorted(range(7))
    assert sorted(result['x']) == [i ** 2 for i in range(7)]
