from psyrun import Param


pspace = Param(x=range(4))
overwrite_dirty = False
exclude_from_result = ['z']


def setup(proc_id):
    assert 0 <= proc_id < 4
    return {'p': 2}


def execute(x, p):
    return {'y': x ** p, 'z': -1}
