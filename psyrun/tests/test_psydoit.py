import os.path
import shutil

import pytest

from psyrun.io import load_dict_h5
from psyrun.psydoit import TaskDef, Config, psydoit
from psyrun.mapper import map_pspace


TASKDIR = os.path.join(os.path.dirname(__file__), 'tasks')


def get_task_path(name):
    return os.path.join(TASKDIR, 'task_' + name + '.py')


def test_load_task_defaults():
    task = TaskDef(get_task_path('square'))
    assert task.path == get_task_path('square')
    assert task.name == 'square'
    assert task.mapper == map_pspace
    assert task.mapper_kwargs == {}
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


class TaskEnv(object):
    def __init__(self, tmpdir):
        self.taskdir = os.path.join(str(tmpdir), 'tasks')
        self.workdir = os.path.join(str(tmpdir), 'work')
        self.dbfile = os.path.join(str(tmpdir), 'doit.db')

        shutil.copytree(TASKDIR, self.taskdir)
        with open(os.path.join(self.taskdir, 'psyconf.py'), 'w') as f:
            f.write('workdir = {0!r}'.format(self.workdir))


@pytest.fixture
def taskenv(tmpdir):
    return TaskEnv(tmpdir)


def test_psydoit(taskenv):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'square'])
    result = load_dict_h5(os.path.join(taskenv.workdir, 'square', 'result.h5'))
    assert sorted(result['y']) == [0, 1, 4, 9]


def test_psydoit_file_dep(taskenv):
    with open(os.path.join(taskenv.taskdir, 'in.txt'), 'w') as f:
        f.write('2')
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'file_dep'])
    result = load_dict_h5(os.path.join(taskenv.workdir, 'file_dep', 'result.h5'))
    assert sorted(result['y']) == [4]

    with open(os.path.join(taskenv.taskdir, 'in.txt'), 'w') as f:
        f.write('3')
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'file_dep'])
    result = load_dict_h5(os.path.join(taskenv.workdir, 'file_dep', 'result.h5'))
    assert sorted(result['y']) == [8]


# TODO: need ability to retrieve job status (and test for it)
# TODO: does not submit jobs that are still queued
# TODO: submits merge job if not queued and result.h5 is missing
# TODO: does not submit processing jobs if job output file is missing, but
# result file is still up-to-date
# TODO: resubmits processing jobs of job output file and result file are missing
# TODO: does not submit split job if job input file is missing, but all job
# output files are up-to-date
# TODO: does not submit split job if job input file is missing, but result file
# is still up-to-date
# TODO: resubmits split job if job input file is missing, job output file is
# missing and result file is missing
