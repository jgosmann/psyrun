from psyrun.pspace import Param
from psyrun.mapper import map_pspace, map_pspace_parallel


def square(a):
    return {'x': a ** 2}


def test_map_pspace():
    calls = []

    def fn(**kwargs):
        calls.append(kwargs)
        return {'result': 42}

    pspace = Param(a=[1, 2])
    result = map_pspace(fn, pspace)

    assert calls == [{'a': 1}, {'a': 2}]
    assert result == {'a': [1, 2], 'result': [42, 42]}


def test_map_pspace_parallel():
    pspace = Param(a=[1, 2])
    result = map_pspace_parallel(square, pspace)
    assert result == {'a': [1, 2], 'x': [1, 4]}
