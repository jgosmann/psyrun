import os
import os.path

import pandas as pd

from psyrun.core import load_results


class SingleItemFanOut(object):
    def __init__(self, workdir):
        self.workdir = workdir
        self.indir = os.path.join(workdir, 'in')
        self.outdir = os.path.join(workdir, 'out')

        if not os.path.exists(self.indir):
            os.makedirs(self.indir)
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

    def _iter_filenames(self, pspace):
        return ('{0}.h5'.format(i) for i in range(len(pspace)))

    def split(self, pspace):
        file_row_mapping = zip(self._iter_filenames(pspace), pspace.iterate())
        for filename, row in file_row_mapping:
            pd.DataFrame([row]).to_hdf(
                os.path.join(self.indir, filename), 'pspace')

    def iter_in_out_files(self, pspace):
        return ((os.path.join(self.indir, f), os.path.join(self.outdir, f))
                for f in self._iter_filenames(pspace))

    def merge(self, merged_filename):
        for filename in os.listdir(self.outdir):
            if os.path.splitext(filename)[1] != '.h5':
                continue
            infile = os.path.join(self.outdir, filename)
            load_results(infile).to_hdf(
                merged_filename, 'results', append=True)
