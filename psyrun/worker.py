from psyrun.io import load_infile, save_outfile
from psyrun.pspace import dict_concat


def get_result(fn, params):
    result = dict(params)
    result.update(fn(**params))
    return result


def map_pspace(fn, pspace):
    return dict_concat(list(get_result(fn, p) for p in pspace.iterate()))


class Worker(object):
    def start(self, fn, infile, outfile):
        raise NotImplementedError()


class SerialWorker(Worker):
    def start(self, fn, infile, outfile):
        pspace = load_infile(infile)
        data = map_pspace(fn, pspace)
        save_outfile(data, outfile)


class ParallelWorker(Worker):
    def __init__(self, n_jobs=-1, backend='multiprocessing'):
        self.n_jobs = n_jobs
        self.backend = backend

    def start(self, fn, infile, outfile):
        import joblib
        pspace = load_infile(infile)
        parallel = joblib.Parallel(n_jobs=self.n_jobs, backend=self.backend)
        data = dict_concat(parallel(
            joblib.delayed(get_result)(fn, p) for p in pspace.iterate()))
        save_outfile(data, outfile)
