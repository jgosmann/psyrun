from psyrun import Param


pspace = Param(x=[2])
file_dep = ['in.txt']


def execute(x):
    with open('in.txt', 'r') as f:
        y = int(f.read())
    return {'y': x ** y}
