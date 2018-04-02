from psyrun import Param
from psyrun.backend import LoadBalancingBackend


pspace = Param(x=range(4))
max_jobs = 2
backend = LoadBalancingBackend
exclude_from_result = ['z']


def setup(proc_id):
    assert proc_id in (0, 1)
    return {'p': 2}


def execute(x, p):
    return {'y': x ** p, 'z': -1}
