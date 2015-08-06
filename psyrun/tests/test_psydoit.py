import os
import os.path
import shutil
import time

import pytest

from psyrun.io import load_dict_h5
from psyrun.psydoit import TaskDef, Config, psydoit
from psyrun.mapper import map_pspace
from psyrun.tests.mockscheduler import MockScheduler


TASKDIR = os.path.join(os.path.dirname(__file__), 'tasks')


class TaskEnv(object):
    def __init__(self, tmpdir):
        self.rootdir = str(tmpdir)
        self.taskdir = os.path.join(str(tmpdir), 'tasks')
        self.workdir = os.path.join(str(tmpdir), 'work')
        self.dbfile = os.path.join(str(tmpdir), 'doit.db')

        shutil.copytree(TASKDIR, self.taskdir)
        with open(os.path.join(self.taskdir, 'psyconf.py'), 'w') as f:
            f.write('workdir = {0!r}'.format(self.workdir))


@pytest.fixture
def taskenv(tmpdir, request):
    env = TaskEnv(tmpdir)
    cwd = os.getcwd()

    def fin():
        os.chdir(cwd)

    request.addfinalizer(fin)
    os.chdir(str(env.rootdir))
    return env


@pytest.fixture
def scheduler(taskenv, request):
    jobfile = os.path.join(taskenv.rootdir, 'jobfile')
    mock = MockScheduler(jobfile)

    def fin():
        try:
            os.remove(jobfile)
        except:
            pass

    request.addfinalizer(fin)
    return mock


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


def test_psydoit(taskenv):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'square'])
    result = load_dict_h5(os.path.join(taskenv.workdir, 'square', 'result.h5'))
    assert sorted(result['y']) == [0, 1, 4, 9]


def test_psydoit_file_dep(taskenv):
    with open(os.path.join(taskenv.taskdir, 'in.txt'), 'w') as f:
        f.write('2')
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'file_dep'])
    result = load_dict_h5(os.path.join(
        taskenv.workdir, 'file_dep', 'result.h5'))
    assert sorted(result['y']) == [4]

    # Ensure that modification time changes as some file systems only support
    # 1s resolution.
    time.sleep(1)

    with open(os.path.join(taskenv.taskdir, 'in.txt'), 'w') as f:
        f.write('3')
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'file_dep'])
    result = load_dict_h5(os.path.join(
        taskenv.workdir, 'file_dep', 'result.h5'))
    assert sorted(result['y']) == [8]


def test_psydoit_does_not_resubmit_queued_jobs(taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    init_job_number = len(scheduler.joblist)
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert init_job_number == len(scheduler.joblist)


def test_psydoit_remerges_if_result_is_missing(taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    os.remove(os.path.join(taskenv.workdir, 'mocked_scheduler', 'result.h5'))

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 1
    assert 'merge' in scheduler.joblist[0]['name']


def test_psydoit_no_resubmits_if_result_is_uptodate(taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    for dirpath, dirnames, filenames in os.walk(taskenv.workdir):
        for filename in filenames:
            if filename == 'result.h5':
                continue
            os.remove(os.path.join(dirpath, filename))

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 0


def test_psydoit_resubmits_for_missing_job_output(taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    os.remove(os.path.join(taskenv.workdir, 'mocked_scheduler', 'result.h5'))
    os.remove(os.path.join(taskenv.workdir, 'mocked_scheduler', 'out', '0.h5'))

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 2
    assert 'process:0' in scheduler.joblist[0]['name']
    assert 'merge' in scheduler.joblist[1]['name']


def test_psydoit_does_not_resubmit_split_if_infiles_uptodate(
        taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    for dirpath, dirnames, filenames in os.walk(taskenv.workdir):
        for filename in filenames:
            if os.path.basename(dirpath) == 'out':
                continue
            os.remove(os.path.join(dirpath, filename))

    psydoit(taskenv.taskdir, ['list', '-s', '--all', '--db-file', taskenv.dbfile, 'mocked_scheduler'])
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    for job in scheduler.joblist:
        assert 'split' not in job['name']


def test_psydoit_resubmits_jobs_if_necessary(taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    shutil.rmtree(taskenv.workdir)

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 3
    assert 'split' in scheduler.joblist[0]['name']
    assert 'process:0' in scheduler.joblist[1]['name']
    assert 'merge' in scheduler.joblist[2]['name']


def test_psydoit_resubmits_merge_if_result_is_outdated(taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))
    os.utime(
        os.path.join(taskenv.workdir, 'mocked_scheduler', 'in', '0.h5'),
        (t, t))
    os.utime(
        os.path.join(taskenv.workdir, 'mocked_scheduler', 'out', '0.h5'),
        (t, t))

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 1
    assert 'merge' in scheduler.joblist[0]['name']


def test_psydoit_resubmits_process_and_merge_if_outfile_is_outdated(
        taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))
    os.utime(
        os.path.join(taskenv.workdir, 'mocked_scheduler', 'in', '0.h5'),
        (t, t))

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 2
    assert 'process:0' in scheduler.joblist[0]['name']
    assert 'merge' in scheduler.joblist[1]['name']


def test_psydoit_resubmits_all_if_infile_is_outdated(
        taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    scheduler.consume()
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 3
    assert 'split' in scheduler.joblist[0]['name']
    assert 'process:0' in scheduler.joblist[1]['name']
    assert 'merge' in scheduler.joblist[2]['name']


def test_psydoit_kills_outdated_jobs(taskenv, scheduler):
    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, 'mocked_scheduler'])
    old_jobs = list(scheduler.joblist)
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))

    psydoit(taskenv.taskdir, ['--db-file', taskenv.dbfile, '-v', '2', 'mocked_scheduler'])
    assert len(scheduler.joblist) == len(old_jobs)
    assert all(x['id'] != y['id'] for x, y in zip(scheduler.joblist, old_jobs))
