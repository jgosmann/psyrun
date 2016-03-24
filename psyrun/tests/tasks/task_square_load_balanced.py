from psyrun import Param
from psyrun.psydoit import LoadBalancingSubtaskCreator


pspace = Param(x=range(4))
max_splits = 2
method = LoadBalancingSubtaskCreator


def execute(x):
    return {'y': x ** 2}
