from psyrun import Param
from psyrun.store.h5 import H5Store


pspace = Param(x=range(4))
store = H5Store()


def execute(x):
    return {'y': x ** 2}
