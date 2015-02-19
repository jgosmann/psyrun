import pandas as pd

from psyrun.core import _get_result, load_infile, save_outfile


class Worker(object):
    def start(self, fn, infile, outfile):
        raise NotImplementedError()


class SerialWorker(Worker):
    def start(self, fn, infile, outfile):
        pspace = load_infile(infile)
        df = pd.concat(_get_result(fn, row) for _, row in pspace.iterrows())
        save_outfile(df, outfile)


class ParallelWorker(Worker):
    def __init__(self, n_jobs=-1, backend='multiprocessing'):
        self.n_jobs = n_jobs
        self.backend = backend

    def start(self, fn, infile, outfile):
        import joblib
        pspace = load_infile(infile)
        parallel = joblib.Parallel(n_jobs=self.n_jobs, backend=self.backend)
        df = pd.concat(parallel(
            joblib.delayed(_get_result)(fn, row)
            for _, row in pspace.iterrows()))
        save_outfile(df, outfile)
