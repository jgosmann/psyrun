from psyrun.io import load_infile, save_outfile


def get_result(fn, params):
    result = dict(params)
    result.update(fn(**params))
    return result


def _concat(args):
    keys = set()
    for a in args:
        keys = keys.union(a.keys())
    return {k: [a.get(k, None) for a in args] for k in keys}


class Worker(object):
    def start(self, fn, infile, outfile):
        raise NotImplementedError()


class SerialWorker(Worker):
    def start(self, fn, infile, outfile):
        pspace = load_infile(infile)
        data = _concat(list(
            get_result(fn, p) for p in pspace.iterate()))
        save_outfile(data, outfile)


class ParallelWorker(Worker):
    def __init__(self, n_jobs=-1, backend='multiprocessing'):
        self.n_jobs = n_jobs
        self.backend = backend

    def start(self, fn, infile, outfile):
        import joblib
        pspace = load_infile(infile)
        parallel = joblib.Parallel(n_jobs=self.n_jobs, backend=self.backend)
        data = _concat(parallel(
            joblib.delayed(get_result)(fn, p) for p in pspace.iterate()))
        save_outfile(data, outfile)
