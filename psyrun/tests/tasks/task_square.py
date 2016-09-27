from psyrun import Param


pspace = Param(x=range(4))
overwrite_dirty = False


def execute(x):
    return {'y': x ** 2}
