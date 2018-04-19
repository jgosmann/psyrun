from __future__ import unicode_literals

import os
import os.path
import re
import shutil
import time

import numpy as np
import pytest

from psyrun.main import psy_main
from psyrun.exceptions import JobsRunningWarning, TaskWorkdirDirtyWarning
from psyrun.store.h5 import H5Store
from psyrun.store.npz import NpzStore
from psyrun.store.pickle import PickleStore
from psyrun.tasks import TaskDef, Config
from psyrun.utils.testing import MockScheduler, TASKDIR, taskenv


@pytest.fixture
def scheduler(taskenv, request):
    jobfile = os.path.join(taskenv.rootdir, 'jobfile')
    mock = MockScheduler(jobfile)

    def fin():
        try:
            os.remove(jobfile)
        except OSError:
            pass

    request.addfinalizer(fin)
    return mock


def get_task_path(name):
    return os.path.join(TASKDIR, 'task_' + name + '.py')


def test_load_task_defaults():
    task = TaskDef(get_task_path('square'))
    assert task.path == get_task_path('square')
    assert task.name == 'square'
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


@pytest.mark.parametrize('task', ['square', 'square_load_balanced'])
class TestPsyrun(object):
    def test_psyrun(self, taskenv, task):
        psy_main(['run', '--taskdir', taskenv.taskdir, task])
        result = PickleStore().load(
            os.path.join(taskenv.workdir, task, 'result.pkl'))
        assert sorted(result['y']) == [0, 1, 4, 9]


def test_psyrun_h5_backend(taskenv):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square_h5'])
    result = H5Store().load(
        os.path.join(taskenv.workdir, 'square_h5', 'result.h5'))
    assert sorted(result['y']) == [0, 1, 4, 9]


def test_psyrun_npz_backend(taskenv):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square_npz'])
    result = NpzStore().load(
        os.path.join(taskenv.workdir, 'square_npz', 'result.npz'))
    assert sorted(result['y']) == [0, 1, 4, 9]


def test_fails_for_existing_old_results_by_default(taskenv):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])
    # Still up to date, not warning
    with pytest.warns(None) as record:
        psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])
    for w in record:
        assert not issubclass(w.category, TaskWorkdirDirtyWarning)
    time.sleep(1)
    os.utime(os.path.join(taskenv.taskdir, 'task_square.py'), None)
    with pytest.warns(TaskWorkdirDirtyWarning):
        psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])


@pytest.mark.parametrize('task', ['square', 'square_load_balanced'])
def test_psyrun_can_continue_interrupted_job(taskenv, task):
    psy_main(['run', '--taskdir', taskenv.taskdir, task])
    result = PickleStore().load(
        os.path.join(taskenv.workdir, task, 'result.pkl'))
    assert sorted(result['y']) == [0, 1, 4, 9]
    time.sleep(1)
    with open(os.path.join(
        taskenv.taskdir, 'task_{}.py'.format(task)), 'a') as f:
        f.write('\npspace += Param(x=[4, 5])\n')
    psy_main(['run', '--taskdir', taskenv.taskdir, '-c', task])
    result = PickleStore().load(
        os.path.join(taskenv.workdir, task, 'result.pkl'))
    assert sorted(result['y']) == [0, 1, 4, 9, 16, 25]


@pytest.mark.parametrize('task', ['square', 'square_load_balanced'])
def test_psyrun_can_continue_job_with_outdated_taskfile(taskenv, task):
    result_file = os.path.join(taskenv.workdir, task, 'result.pkl')
    psy_main(['run', '--taskdir', taskenv.taskdir, task])
    result = PickleStore().load(result_file)
    assert sorted(result['y']) == [0, 1, 4, 9]
    with open(os.path.join(
        taskenv.taskdir, 'task_{}.py'.format(task)), 'a') as f:
        f.write('\npspace += Param(x=[4, 5])\n')
    psy_main(['status', '--taskdir', taskenv.taskdir,  task])
    os.utime(result_file, None)
    psy_main(['run', '--taskdir', taskenv.taskdir, '-c', task])
    result = PickleStore().load(result_file)
    assert sorted(result['y']) == [0, 1, 4, 9, 16, 25]


def test_psyrun_can_continue_interrupted_job_no_result_file(taskenv):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])
    result = PickleStore().load(
        os.path.join(taskenv.workdir, 'square', 'result.pkl'))
    assert sorted(result['y']) == [0, 1, 4, 9]
    with open(os.path.join(taskenv.taskdir, 'task_square.py'), 'a') as f:
        f.write('\npspace += Param(x=[4, 5])\n')
    os.remove(os.path.join(taskenv.workdir, 'square', 'result.pkl'))
    psy_main(['run', '--taskdir', taskenv.taskdir, '-c', 'square'])
    result = PickleStore().load(
        os.path.join(taskenv.workdir, 'square', 'result.pkl'))
    assert sorted(result['y']) == [0, 1, 4, 9, 16, 25]


def test_allows_to_clean_results(taskenv):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])
    time.sleep(1)
    os.utime(os.path.join(taskenv.taskdir, 'task_square.py'), None)
    psy_main(['clean', '--taskdir', taskenv.taskdir, 'square'])
    with pytest.warns(None) as record:
        psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])
    for w in record:
        assert not issubclass(w.category, TaskWorkdirDirtyWarning)


def test_psyrun_workdir_contents(taskenv):
    workdir = os.path.join('psy-work', 'square')
    os.remove(os.path.join(taskenv.taskdir, 'psy-conf.py'))
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])
    assert os.path.exists(os.path.join(workdir, 'in', '0.pkl'))
    assert os.path.exists(os.path.join(workdir, 'out', '0.pkl'))
    assert os.path.exists(os.path.join(workdir, 'result.pkl'))
    assert os.path.exists(os.path.join(workdir, 'square:split.py'))
    assert os.path.exists(os.path.join(workdir, 'square:process.py'))
    assert os.path.exists(os.path.join(workdir, 'square:merge.py'))
    assert os.path.exists(os.path.join(workdir, 'square:split.log'))
    assert os.path.exists(os.path.join(workdir, 'square:process:0.log'))
    assert os.path.exists(os.path.join(workdir, 'square:merge.log'))


def test_psyrun_workdir_contents_load_balanced(taskenv):
    workdir = os.path.join('psy-work', 'square_load_balanced')
    os.remove(os.path.join(taskenv.taskdir, 'psy-conf.py'))
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square_load_balanced'])
    assert os.path.exists(os.path.join(workdir, 'in.pkl'))
    assert os.path.exists(os.path.join(workdir, 'result.pkl'))
    assert os.path.exists(os.path.join(
        workdir, 'square_load_balanced:pspace.py'))
    assert os.path.exists(os.path.join(
        workdir, 'square_load_balanced:process.py'))
    assert os.path.exists(os.path.join(
        workdir, 'square_load_balanced:process:0.log'))
    assert os.path.exists(os.path.join(
        workdir, 'square_load_balanced:process:1.log'))


def test_psyrun_file_dep(taskenv):
    with open(os.path.join(taskenv.taskdir, 'in.txt'), 'w') as f:
        f.write('2')
    psy_main(['run', '--taskdir', taskenv.taskdir, 'file_dep'])
    result = PickleStore().load(os.path.join(
        taskenv.workdir, 'file_dep', 'result.pkl'))
    assert sorted(result['y']) == [4]

    # Ensure that modification time changes as some file systems only support
    # 1s resolution.
    time.sleep(1)

    with open(os.path.join(taskenv.taskdir, 'in.txt'), 'w') as f:
        f.write('3')
    psy_main(['run', '--taskdir', taskenv.taskdir, 'file_dep'])
    result = PickleStore().load(os.path.join(
        taskenv.workdir, 'file_dep', 'result.pkl'))
    assert sorted(result['y']) == [8]


@pytest.mark.filterwarnings(JobsRunningWarning)
def test_psyrun_does_not_resubmit_queued_jobs(taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    expected = scheduler.joblist
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(expected) == len(scheduler.joblist)
    assert all(x['id'] == y['id'] for x, y in zip(expected, scheduler.joblist))


def test_psyrun_remerges_if_result_is_missing(taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    os.remove(os.path.join(taskenv.workdir, 'mocked_scheduler', 'result.pkl'))

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 1
    assert 'merge' in scheduler.joblist[0]['name']


def test_psyrun_no_resubmits_if_result_is_uptodate(taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    for dirpath, _, filenames in os.walk(taskenv.workdir):
        for filename in filenames:
            if filename == 'result.pkl':
                continue
            os.remove(os.path.join(dirpath, filename))

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 0


def test_psyrun_resubmits_for_missing_job_output(taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    os.remove(os.path.join(taskenv.workdir, 'mocked_scheduler', 'result.pkl'))
    os.remove(os.path.join(
        taskenv.workdir, 'mocked_scheduler', 'out', '0.pkl'))

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 2
    assert 'process:0' in scheduler.joblist[0]['name']
    assert 'merge' in scheduler.joblist[1]['name']


def test_psyrun_does_not_resubmit_split_if_infiles_uptodate(
        taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    for dirpath, _, filenames in os.walk(taskenv.workdir):
        for filename in filenames:
            if os.path.basename(dirpath) == 'out':
                continue
            os.remove(os.path.join(dirpath, filename))

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    for job in scheduler.joblist:
        assert 'split' not in job['name']


def test_psyrun_resubmits_jobs_if_necessary(taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    shutil.rmtree(taskenv.workdir)

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 6
    assert 'split' in scheduler.joblist[0]['name']
    for i in range(4):
        assert 'process:{0}'.format(i) in scheduler.joblist[i + 1]['name']
    assert 'merge' in scheduler.joblist[5]['name']


def test_psyrun_shows_error_if_resubmit_of_queued_job_necessary(
        taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume_job(scheduler.joblist[0])
    scheduler.consume_job(scheduler.joblist[1])
    expected = scheduler.joblist
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))

    with pytest.warns(JobsRunningWarning):
        psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(expected) == len(scheduler.joblist)
    assert all(x['id'] == y['id'] for x, y in zip(expected, scheduler.joblist))


def test_psyrun_resubmits_merge_if_result_is_outdated(taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))
    for i in range(4):
        os.utime(os.path.join(
            taskenv.workdir, 'mocked_scheduler', 'in',
            str(i) + '.pkl'), (t, t))
        os.utime(os.path.join(
            taskenv.workdir, 'mocked_scheduler', 'out',
            str(i) + '.pkl'), (t, t))

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 1
    assert 'merge' in scheduler.joblist[0]['name']


def test_psyrun_resubmits_process_and_merge_if_outfile_is_outdated(
        taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))
    for i in range(4):
        os.utime(os.path.join(
            taskenv.workdir, 'mocked_scheduler', 'in',
            str(i) + '.pkl'), (t, t))

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 5
    for i in range(4):
        assert 'process:{0}'.format(i) in scheduler.joblist[i]['name']
    assert 'merge' in scheduler.joblist[4]['name']


def test_psyrun_resubmits_all_if_infile_is_outdated(
        taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    scheduler.consume()
    time.sleep(1)
    t = time.time()
    os.utime(
        os.path.join(taskenv.taskdir, 'task_mocked_scheduler.py'),
        (t, t))

    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 6
    assert 'split' in scheduler.joblist[0]['name']
    for i in range(4):
        assert 'process:{0}'.format(i) in scheduler.joblist[i + 1]['name']
    assert 'merge' in scheduler.joblist[5]['name']


def test_multiple_splits(taskenv):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square2'])
    result = PickleStore().load(os.path.join(
        taskenv.workdir, 'square2', 'result.pkl'))
    assert sorted(result['y']) == [0, 1, 4, 9]


def test_psy_run_runs_all_tasks(taskenv):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'square', 'square2'])
    result = PickleStore().load(
        os.path.join(taskenv.workdir, 'square', 'result.pkl'))
    assert sorted(result['y']) == [0, 1, 4, 9]
    result = PickleStore().load(os.path.join(
        taskenv.workdir, 'square2', 'result.pkl'))
    assert sorted(result['y']) == [0, 1, 4, 9]


def test_psy_list(taskenv, capsys):
    expected = set()
    for entry in os.listdir(taskenv.taskdir):
        m = re.match(r'^task_(.*)\.py$', entry)
        if m:
            expected.add(m.group(1))

    psy_main(['list', '--taskdir', taskenv.taskdir])
    out, _ = capsys.readouterr()
    listed = {x.strip() for x in out.split('\n') if x != ''}
    assert listed == expected


def test_psy_status(taskenv, capsys):
    psy_main(['status', '--taskdir', taskenv.taskdir, 'square'])
    out, _ = capsys.readouterr()
    assert out == """square:
  0 out of 4 rows completed.

"""

    psy_main(['status', '--taskdir', taskenv.taskdir, '-v', 'square'])
    out, _ = capsys.readouterr()
    assert out == """square:
  0 out of 4 rows completed.
  Missing parameter sets:
    {'x': 0}
    {'x': 1}
    {'x': 2}
    {'x': 3}
  Queued parameter sets:
  No failed jobs.

"""

    psy_main(['run', '--taskdir', taskenv.taskdir, 'square'])
    out, _ = capsys.readouterr()
    psy_main(['status', '--taskdir', taskenv.taskdir, '-v', 'square'])
    out, _ = capsys.readouterr()
    assert out == """square:
  4 out of 4 rows completed.
  Missing parameter sets:
  Queued parameter sets:
  No failed jobs.

"""

    os.remove(os.path.join(taskenv.workdir, 'square', 'out', '2.pkl'))
    psy_main(['status', '--taskdir', taskenv.taskdir, '-v', 'square'])
    out, _ = capsys.readouterr()
    assert out == """square:
  4 out of 4 rows completed.
  Missing parameter sets:
  Queued parameter sets:
  Failed jobs:
    square:process:2

"""

    os.remove(os.path.join(taskenv.workdir, 'square', 'result.pkl'))
    psy_main(['status', '--taskdir', taskenv.taskdir, '-v', 'square'])
    out, _ = capsys.readouterr()
    assert out == """square:
  3 out of 4 rows completed.
  Missing parameter sets:
    {'x': 2}
  Queued parameter sets:
  Failed jobs:
    square:process:2

"""


def test_psy_status_load_balanced(taskenv, capsys):
    psy_main(['status', '--taskdir', taskenv.taskdir, 'square_load_balanced'])
    out, _ = capsys.readouterr()
    assert out == """square_load_balanced:
  0 out of 4 rows completed.

"""

    psy_main(['status', '--taskdir', taskenv.taskdir, '-v',
              'square_load_balanced'])
    out, _ = capsys.readouterr()
    assert out == """square_load_balanced:
  0 out of 4 rows completed.
  Missing parameter sets:
    {'x': 0}
    {'x': 1}
    {'x': 2}
    {'x': 3}

"""

    psy_main(['run', '--taskdir', taskenv.taskdir, 'square_load_balanced'])
    out, _ = capsys.readouterr()
    psy_main(['status', '--taskdir', taskenv.taskdir, '-v',
              'square_load_balanced'])
    out, _ = capsys.readouterr()
    assert out == """square_load_balanced:
  4 out of 4 rows completed.
  Missing parameter sets:

"""


def test_psy_kill(taskenv, scheduler):
    psy_main(['run', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) > 0
    psy_main(['kill', '--taskdir', taskenv.taskdir, 'mocked_scheduler'])
    assert len(scheduler.joblist) == 0


@pytest.mark.parametrize('store', [PickleStore(), NpzStore(), H5Store()])
def test_psy_merge(tmpdir, store):
    resultfile = os.path.join(str(tmpdir), 'result' + store.ext)
    outdir = os.path.join(str(tmpdir), 'out')
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    data_segments = ({'x': [1]}, {'x': [2]})
    for i, d in enumerate(data_segments):
        store.save(os.path.join(outdir, str(i) + store.ext), d)

    psy_main(['merge', outdir, resultfile])
    merged = store.load(resultfile)
    assert list(merged.keys()) == ['x']
    assert np.all(np.asarray(sorted(merged['x'])) == np.array([1, 2]))


def test_new_task(taskenv):
    psy_main(['new-task', '--taskdir', taskenv.taskdir, 'new-task-test'])
    assert os.path.exists(
        os.path.join(taskenv.taskdir, 'task_new-task-test.py'))
    assert psy_main(
        ['new-task', '--taskdir', taskenv.taskdir, 'new-task-test']) != 0


@pytest.mark.parametrize('scheduler_str', ['psyrun.scheduler.Sqsub', 'Sqsub'])
def test_new_task_scheduler_arg(taskenv, scheduler_str):
    psy_main(['new-task', '--taskdir', taskenv.taskdir, '-s', scheduler_str,
              'new-task-test'])
    path = os.path.join(taskenv.taskdir, 'task_new-task-test.py')
    with open(path, 'r') as f:
        data = f.read()
    assert 'from psyrun.scheduler import Sqsub' in data
    assert 'scheduler = Sqsub' in data
    assert 'scheduler_args = {' in data
    assert 'scheduler_args = {}' not in data
