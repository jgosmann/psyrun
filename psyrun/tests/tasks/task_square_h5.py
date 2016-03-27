from psyrun import Param
from psyrun.store import H5Store


pspace = Param(x=range(4))
io = H5Store()


def execute(x):
    return {'y': x ** 2}
