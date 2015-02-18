import os
import os.path

import pandas as pd

from psyrun.core import load_results


class SingleItemFanOut(object):
    def __init__(self, workdir):
        self.indir = os.path.join(workdir, 'in')
        self.outdir = os.path.join(workdir, 'out')
        self.queue = []

        if not os.path.exists(self.indir):
            os.makedirs(self.indir)
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

    def split(self, pspace):
        for i, row in enumerate(pspace.iterate()):
            filename = '{0}.h5'.format(i)
            pd.DataFrame([row]).to_hdf(
                os.path.join(self.indir, filename), 'pspace')
            self.queue.append(filename)

    def iter_in_out_files(self):
        return ((os.path.join(self.indir, f), os.path.join(self.outdir, f))
                for f in self.queue)

    def merge(self, merged_filename):
        for filename in os.listdir(self.outdir):
            if os.path.splitext(filename)[1] != '.h5':
                continue
            infile = os.path.join(self.outdir, filename)
            load_results(infile).to_hdf(
                merged_filename, 'results', append=True)
