from psyrun import Param
from psyrun.tests.mockscheduler import MockScheduler


scheduler = MockScheduler('jobfile')
pspace = Param(x=range(4))


def execute(x):
    return {'y': x ** 2}
