import numpy as np

from psyrun.io import load_infile, save_infile
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
