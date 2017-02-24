from psyrun import Param


pspace = Param(x=[2])
file_dep = ['in.txt']
overwrite_dirty = True


def execute(x):
    with open('in.txt', 'r') as f:
        y = int(f.read())
    return {'y': x ** y}
