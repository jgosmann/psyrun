from psyrun import Param


pspace = Param(x=range(4))
min_items = 1


def execute(x):
    return {'y': x ** 2}
