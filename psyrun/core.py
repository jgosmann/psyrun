from joblib import Parallel, delayed
import pandas as pd


def _get_result(fn, row, *args, **kwargs):
    result = fn(row, *args, **kwargs)
    for k, v in row.iteritems():
        result[k] = v
    return result


def dispatch_threaded(fn, pspace, args, kwargs, n_threads=-1):
    return pd.concat(Parallel(n_jobs=n_threads)(
        delayed(_get_result)(fn, row, *args, **kwargs)
        for _, row in pspace.iterrows()))
