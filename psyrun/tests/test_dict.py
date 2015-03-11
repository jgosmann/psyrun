import pytest

from psyrun.dict import dict_concat


@pytest.mark.parametrize('args,result', [
    (({0: 0}, {0: 1}, {0: 2}), {0: [0, 1, 2]}),
    (({0: 2},), {0: [2]}),
    (({},), {}),
    (({0: 0, 1: 1}, {0: 0, 1: 1}), {0: [0, 0], 1: [1, 1]}),
    ((), {}),
    (({0: 0}, {1: 1}), {0: [0, None], 1: [None, 1]})
])
def test_dict_concat(args, result):
    assert dict_concat(args) == result
