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


def _limited_iter(limit, it):
    for _ in range(limit):
        yield next(it)


def _get_pspace_dir(base):
    path = os.path.join(base, 'in')
    if not os.path.exists(path):
        os.mkdir(path)
    return path


def _get_results_dir(base):
    path = os.path.join(base, 'out')
    if not os.path.exists(path):
        os.mkdir(path)
    return path


def get_n_nodes(psize, n_nodes=None, n_jobs=-1):
    if n_jobs < 0:
        try:
            n_jobs = multiprocessing.cpu_count()
        except NotImplementedError:
            n_jobs = 1

    if n_nodes is None:
        n_nodes = psize // n_jobs
        if psize % n_jobs > 0:
            n_nodes += 1
    return n_nodes


# FIXME create distribute dir and handle existing
def prepare_distribute(pspace, distribute_dir, n_nodes=None, n_jobs=-1):
    if n_jobs < 0:
        try:
            n_jobs = multiprocessing.cpu_count()
        except NotImplementedError:
            n_jobs = 1

    psize = len(pspace)
    if n_nodes is None:
        blocksize = n_jobs
        n_nodes = get_n_nodes(psize, n_nodes, n_jobs)
    else:
        blocksize = (psize + psize % n_nodes) // n_nodes

    it = pspace.iterate()
    for i in range(n_nodes):
        block = pd.DataFrame(_limited_iter(blocksize, it))
        block.to_hdf(os.path.join(
            _get_pspace_dir(distribute_dir), '{0}.h5'.format(i)), 'pspace')


def dispatch_distributed(distribute_dir, launcher):
    for filename in os.listdir(_get_pspace_dir(distribute_dir)):
        if os.path.splitext(filename)[1] != '.h5':
            continue
        infile = os.path.join(_get_pspace_dir(distribute_dir), filename)
        outfile = os.path.join(_get_results_dir(distribute_dir), filename)
        launcher(infile, outfile)


def merge_results(distribute_dir, merged_filename):
    for filename in os.listdir(_get_results_dir(distribute_dir)):
        if os.path.splitext(filename)[1] != '.h5':
            continue
        infile = os.path.join(_get_results_dir(distribute_dir), filename)
        load_results(infile).to_hdf(merged_filename, 'results', append=True)
