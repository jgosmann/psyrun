import os.path

import pandas as pd
import pytest

from psyrun import Param
from psyrun.core import (
    dispatch_threaded, make_local_fn_launcher, load_infile, save_outfile,
    load_results, prepare_distribute, dispatch_distributed, merge_results)


def fn_df(params, arg, foo):
    return pd.DataFrame({
        'x': [params['a'] ** 2, params['a'] ** 3],
        'arg': arg, 'foo': foo})


@pytest.mark.parametrize('n_threads', [-1, 1, 2])
def test_dispatch_threaded_returning_df(n_threads):
    space = Param(a=[1, 2, 3]).build()
    args = [23]
    kwargs = {'foo': 'bar'}
    result = dispatch_threaded(
        fn_df, space, args=args, kwargs=kwargs, n_threads=n_threads)

    assert sorted(result['a']) == [1, 1, 2, 2, 3, 3]
    assert sorted(result['x']) == [1, 1, 4, 8, 9, 27]
    assert sorted(result['arg']) == [23] * 6
    assert sorted(result['foo']) == ['bar'] * 6


def square(pspace):
    return pd.DataFrame({'x': [pspace['a'] ** 2]})


def test_distribute(tmpdir):
    result_path = os.path.join(str(tmpdir), 'result')
    space = Param(a=range(7))
    prepare_distribute(space, str(tmpdir), n_jobs=3)
    dispatch_distributed(
        str(tmpdir), make_local_fn_launcher(square, n_threads=3))
    merge_results(str(tmpdir), result_path)
    result = load_results(result_path)
    assert sorted(result['a']) == sorted(range(7))
    assert sorted(result['x']) == [i ** 2 for i in range(7)]
