import os
import os.path

import pytest

from psyrun.io import load_dict_h5, save_dict_h5
from psyrun.pspace import Param
from psyrun.mapper import map_pspace, map_pspace_parallel
from psyrun.processing import Splitter, Worker


def square(a):
    return {'x': a ** 2}


@pytest.mark.parametrize(
    'pspace_size,max_splits,min_items,n_splits', [
        (7, 4, 4, 2), (8, 4, 4, 2), (9, 4, 4, 3),
        (15, 4, 4, 4), (16, 4, 4, 4), (17, 4, 4, 4),
        (15, 2, 4, 2), (16, 4, 16, 1)
    ])
class TestSplitter(object):
    def test_n_splits(
            self, tmpdir, pspace_size, max_splits, min_items, n_splits):
        splitter = Splitter(
            str(tmpdir), Param(x=range(pspace_size)), max_splits, min_items)
        assert splitter.n_splits == n_splits
        assert len(list(splitter.iter_in_out_files())) == n_splits

    def test_split_merge_roundtrip(
            self, tmpdir, pspace_size, max_splits, min_items, n_splits):
        splitter = Splitter(
            str(tmpdir), Param(x=range(pspace_size)), max_splits, min_items)
        splitter.split()

        for filename in os.listdir(splitter.indir):
            infile = os.path.join(splitter.indir, filename)
            outfile = os.path.join(splitter.outdir, filename)
            save_dict_h5(outfile, load_dict_h5(infile))

        result_file = os.path.join(str(tmpdir), 'result.h5')
        Splitter.merge(splitter.outdir, result_file)
        result = load_dict_h5(result_file)
        assert sorted(result['x']) == sorted(range(pspace_size))


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