from psyrun import Param
from psyrun.backend import LoadBalancingBackend


pspace = Param(x=range(4))
max_jobs = 2
backend = LoadBalancingBackend


def execute(x):
    return {'y': x ** 2}
