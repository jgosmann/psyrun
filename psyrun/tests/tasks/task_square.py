import pandas as pd

from psyrun import Param


pspace = Param(x=range(4))


def execute(x):
    return {'y': [x ** 2]}
