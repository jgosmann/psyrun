import pandas as pd

from psyrun import Param


pspace = Param(x=range(4))
n_nodes = 1
n_jobs = 2


def execute(params):
    return pd.DataFrame({'y': [params['x'] ** 2]})
