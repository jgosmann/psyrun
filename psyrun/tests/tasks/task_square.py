import pandas as pd

from psyrun import Param


pspace = Param(x=range(4))


def execute(params):
    return pd.DataFrame({'y': params['x'] ** 2})
