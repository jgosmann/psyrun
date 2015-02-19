import pandas as pd

from psyrun.io import load_infile, save_outfile


def get_result(fn, params):
    result = fn(**{k: v[0] for k, v in params.reset_index(
        drop=True).to_dict().items()})
    for k, v in params.iteritems():
        result[k] = v
    return pd.DataFrame(result)


class Worker(object):
    def start(self, fn, infile, outfile):
        raise NotImplementedError()

    def iter_pspace(self, pspace):
        return (pspace.iloc[i:i + 1] for i in range(len(pspace)))


class SerialWorker(Worker):
    def start(self, fn, infile, outfile):
        pspace = load_infile(infile)
        df = pd.concat(get_result(fn, p) for p in self.iter_pspace(pspace))
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
            joblib.delayed(get_result)(fn, p)
            for p in self.iter_pspace(pspace)))
        save_outfile(df, outfile)
