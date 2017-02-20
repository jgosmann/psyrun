from psyrun import Param
from psyrun.store import NpzStore


pspace = Param(x=range(4))
store = NpzStore()


def execute(x):
    return {'y': x ** 2}
