import os
import os.path

import pytest

from psyrun.store import DefaultStore
from psyrun.backend.distribute import Splitter, Worker
from psyrun.pspace import Param


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
            DefaultStore().save(outfile, DefaultStore().load(infile))

        result_file = os.path.join(str(tmpdir), 'result.npz')
        Splitter.merge(splitter.outdir, result_file)
        result = DefaultStore().load(result_file)
        assert sorted(result['x']) == sorted(range(pspace_size))


def test_worker(tmpdir):
    infile = str(tmpdir.join('in.npz'))
    outfile = str(tmpdir.join('out.npz'))
    DefaultStore().save(infile, Param(a=range(7)).build())
    worker = Worker(DefaultStore())
    worker.start(square, infile, outfile)
    result = DefaultStore().load(outfile)
    assert sorted(result['a']) == sorted(range(7))
    assert sorted(result['x']) == [i ** 2 for i in range(7)]