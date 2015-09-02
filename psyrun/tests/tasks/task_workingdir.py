import os.path
import sys
from psyrun import Param


pspace = Param(x=[1])


def execute(x):
    assert os.path.abspath(os.path.dirname(__file__)) is sys.path
    return {'y': 1}
