import os.path

import pytest

from psyrun.io import load_dict_h5, save_dict_h5
from psyrun.pspace import Param
from psyrun.mapper import map_pspace, map_pspace_parallel
from psyrun.worker import Worker


def square(a):
    return {'x': a ** 2}


@pytest.mark.parametrize('mapper', [map_pspace, map_pspace_parallel])
def test_worker(mapper, tmpdir):
    infile = os.path.join(str(tmpdir), 'in.h5')
    outfile = os.path.join(str(tmpdir), 'out.h5')
    save_dict_h5(infile, Param(a=range(7)).build())
    worker = Worker(mapper)
    worker.start(square, infile, outfile)
    result = load_dict_h5(outfile)
    assert sorted(result['a']) == sorted(range(7))
    assert sorted(result['x']) == [i ** 2 for i in range(7)]
