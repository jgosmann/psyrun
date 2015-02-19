import os.path

import pandas as pd

from psyrun import Param
from psyrun.core import (
    load_results, prepare_distribute, dispatch_distributed, merge_results)


def fn_df(params, arg, foo):
    return pd.DataFrame({
        'x': [params['a'] ** 2, params['a'] ** 3],
        'arg': arg, 'foo': foo})


def square(pspace):
    return pd.DataFrame({'x': [pspace['a'] ** 2]})


#def test_distribute(tmpdir):
    #result_path = os.path.join(str(tmpdir), 'result')
    #space = Param(a=range(7))
    #prepare_distribute(space, str(tmpdir), n_jobs=3)
    #dispatch_distributed(
        #str(tmpdir), make_local_fn_launcher(square, n_threads=3))
    #merge_results(str(tmpdir), result_path)
    #result = load_results(result_path)
    #assert sorted(result['a']) == sorted(range(7))
    #assert sorted(result['x']) == [i ** 2 for i in range(7)]
