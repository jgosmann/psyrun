import pandas as pd

from psyrun.core import _get_result, load_infile, save_outfile


class SerialWorker(object):
    def start(self, fn, infile, outfile):
        pspace = load_infile(infile)
        df = pd.concat(_get_result(fn, row) for _, row in pspace.iterrows())
        save_outfile(df, outfile)
