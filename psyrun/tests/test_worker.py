import os.path

import pandas as pd
import pytest

from psyrun.io import load_results, save_infile
from psyrun.pspace import Param
from psyrun.worker import ParallelWorker, SerialWorker


def square(a):
    return {'x': [a ** 2]}


@pytest.mark.parametrize('worker', [SerialWorker(), ParallelWorker()])
def test_worker(worker, tmpdir):
    infile = os.path.join(str(tmpdir), 'in.h5')
    outfile = os.path.join(str(tmpdir), 'out.h5')
    save_infile(Param(a=range(7)).build(), infile)
    worker.start(square, infile, outfile)
    result = load_results(outfile)
    assert sorted(result['a']) == sorted(range(7))
    assert sorted(result['x']) == [i ** 2 for i in range(7)]
