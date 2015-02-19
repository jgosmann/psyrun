import pandas as pd

from psyrun.io import load_infile, save_outfile


def get_result(fn, row, *args, **kwargs):
    result = fn(row, *args, **kwargs)
    for k, v in row.iteritems():
        result[k] = v
    return result


class Worker(object):
    def start(self, fn, infile, outfile):
        raise NotImplementedError()


class SerialWorker(Worker):
    def start(self, fn, infile, outfile):
        pspace = load_infile(infile)
        df = pd.concat(get_result(fn, pspace.iloc[i:i + 1])
                       for i in range(len(pspace)))
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
            joblib.delayed(get_result)(fn, pspace.iloc[i:i + 1])
            for i in range(len(pspace))))
        save_outfile(df, outfile)
