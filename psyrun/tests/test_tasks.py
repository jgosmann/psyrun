import os.path

from psyrun.core import load_results
from psyrun.psydoit import psydoit


def test_psydoit(tmpdir):
    dbfile = os.path.join(str(tmpdir), 'doit.db')
    taskdir = os.path.join(os.path.dirname(__file__), 'tasks')
    psydoit(taskdir, str(tmpdir), ['--db-file', dbfile])
    result = load_results(os.path.join(str(tmpdir), 'result.h5'))
    assert sorted(result['y']) == [0, 1, 4, 9]
