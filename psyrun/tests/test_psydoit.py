import os.path

from psyrun.io import load_results
from psyrun.psydoit import TaskDef, Config, psydoit
from psyrun.worker import Worker


TASKDIR = os.path.join(os.path.dirname(__file__), 'tasks')


def get_task_path(name):
    return os.path.join(TASKDIR, 'task_' + name + '.py')


def test_load_task_defaults():
    task = TaskDef(get_task_path('square'))
    assert task.path == get_task_path('square')
    assert task.name == 'square'
    assert isinstance(task.worker, Worker)
    assert hasattr(task, 'scheduler')
    assert hasattr(task, 'scheduler_args')
    assert hasattr(task, 'python')


def test_load_task_uses_config_as_default():
    conf = Config()
    conf.python = 'env python'
    task1 = TaskDef(get_task_path('square'), conf)
    assert task1.python == 'env python'
    task2 = TaskDef(get_task_path('noop'), conf)
    assert task2.python == 'true'


def test_load_config_from_file(tmpdir):
    conffile = os.path.join(str(tmpdir), 'conf.py')
    with open(conffile, 'w') as f:
        f.write('python = "env python"')
    conf = Config.load_from_file(conffile)
    assert conf.python == 'env python'


def test_psydoit(tmpdir):
    dbfile = os.path.join(str(tmpdir), 'doit.db')
    psydoit(TASKDIR, str(tmpdir), ['--db-file', dbfile])
    result = load_results(os.path.join(str(tmpdir), 'square', 'result.h5'))
    assert sorted(result['y']) == [0, 1, 4, 9]
