import os.path

from psyrun.core import load_results
from psyrun.psydoit import load_task, psydoit
from psyrun.worker import Worker


TASKDIR = os.path.join(os.path.dirname(__file__), 'tasks')


def test_load_task_defaults():
    task = load_task(TASKDIR, 'square')
    assert task.taskdir == TASKDIR
    assert task.name == 'square'
    assert isinstance(task.worker, Worker)
    assert hasattr(task, 'scheduler')
    assert hasattr(task, 'python')


def test_psydoit(tmpdir):
    dbfile = os.path.join(str(tmpdir), 'doit.db')
    psydoit(TASKDIR, str(tmpdir), ['--db-file', dbfile])
    result = load_results(os.path.join(str(tmpdir), 'square', 'result.h5'))
    assert sorted(result['y']) == [0, 1, 4, 9]
