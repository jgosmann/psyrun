import os
import os.path

import pandas as pd

from psyrun.io import load_results


class Splitter(object):
    def __init__(self, workdir, pspace, max_splits=64, min_items=4):
        self.workdir = workdir
        self.indir = self._get_indir(workdir)
        self.outdir = self._get_outdir(workdir)

        if not os.path.exists(self.indir):
            os.makedirs(self.indir)
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        self.pspace = pspace
        self.max_splits = max_splits
        self.min_items = min_items

    @property
    def n_splits(self):
        return min(
            self.max_splits, (len(self.pspace) - 1) // self.min_items + 1)

    def split(self):
        items_remaining = len(self.pspace)
        param_iter = self.pspace.iterate()
        for i, filename in enumerate(self._iter_filenames()):
            split_size = max(
                self.min_items, items_remaining // (self.max_splits - i))
            items_remaining -= split_size
            df = pd.concat(
                row for row in self._iter_n(param_iter, split_size)).to_hdf(
                    os.path.join(self.indir, filename), 'pspace')

    @classmethod
    def merge(cls, workdir, merged_filename):
        outdir = cls._get_outdir(workdir)
        for filename in os.listdir(outdir):
            if os.path.splitext(filename)[1] != '.h5':
                continue
            infile = os.path.join(outdir, filename)
            load_results(infile).to_hdf(
                merged_filename, 'results', append=True)

    def iter_in_out_files(self):
        return ((os.path.join(self.indir, f), os.path.join(self.outdir, f))
                for f in self._iter_filenames())

    def _iter_filenames(self):
        return ('{0}.h5'.format(i) for i in range(self.n_splits))

    @staticmethod
    def _iter_n(it, n):
        for _ in range(n):
            yield next(it)

    @classmethod
    def _get_indir(cls, workdir):
        return os.path.join(workdir, 'in')

    @classmethod
    def _get_outdir(cls, workdir):
        return os.path.join(workdir, 'out')
