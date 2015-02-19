import multiprocessing
import os
import os.path

from joblib import Parallel, delayed
import pandas as pd


def _get_result(fn, row, *args, **kwargs):
    result = fn(row, *args, **kwargs)
    for k, v in row.iteritems():
        result[k] = v
    return result


def save_infile(df, infile):
    return df.to_hdf(infile, 'pspace')


def load_infile(infile):
    return pd.read_hdf(infile, 'pspace')


def save_outfile(df, outfile):
    return df.to_hdf(outfile, 'results')


def load_results(filename):
    return pd.read_hdf(filename, 'results')
