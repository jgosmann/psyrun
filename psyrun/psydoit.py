import pkgutil
import os
import os.path
import re
import sys

from doit.task import dict_to_task
from doit.cmd_base import TaskLoader
from doit.doit_cmd import DoitMain

from psyrun.fanout import SingleItemFanOut
from psyrun.scheduler import ImmediateRun


def load_task(taskdir, name):
    module_name = 'task_' + name
    task = pkgutil.ImpImporter(taskdir).find_module(module_name).load_module(
        module_name)
    setattr(task, 'taskdir', taskdir)
    setattr(task, 'name', name)
    if not hasattr(task, 'scheduler'):
        setattr(task, 'scheduler', ImmediateRun())
    if not hasattr(task, 'python'):
        setattr(task, 'python', 'python')
    return task


class PackageLoader(TaskLoader):
    TASK_PATTERN = re.compile(r'^task_(.*)$')

    def __init__(self, taskdir, workdir):
        self.taskdir = taskdir
        self.workdir = workdir

    def load_tasks(self, cmd, opt_values, pos_args):
        task_list = []
        for _, module_name, _ in pkgutil.iter_modules([self.taskdir]):
            m = self.TASK_PATTERN.match(module_name)
            if m:
                task = load_task(self.taskdir, m.group(1))
                task_list.extend(self.create_task(task))
        return task_list, {}

    def create_task(self, task):
        creator = FanOutSubtaskCreator(self.workdir, task)
        yield creator.create_split_subtask()
        for st in creator.create_process_subtasks():
            yield st
        yield creator.create_merge_subtask()


class FanOutSubtaskCreator(object):
    def __init__(self, workdir, task):
        self.fanout = SingleItemFanOut(os.path.join(workdir, task.name))
        self.task = task

    def _submit(self, code, depends_on=None):
        code = '''
from psyrun.psydoit import load_task
task = load_task({taskdir!r}, {name!r})
{code}
        '''.format(taskdir=self.task.taskdir, name=self.task.name, code=code)
        # FIXME depends_on
        return {'id': self.task.scheduler.submit(
            [self.task.python, '-c', code])}

    def create_split_subtask(self):
        code = '''
from psyrun.fanout import SingleItemFanOut as FanOut
FanOut({workdir!r}).split(task.pspace)
        '''.format(workdir=self.fanout.workdir)

        return dict_to_task({
            'name': self.task.name + ':split',
            'file_dep': [self.task.__file__],
            'targets': [f for f, _ in self.fanout.iter_in_out_files(
                self.task.pspace)],
            'actions': [(self._submit, [code])],
        })

    def create_process_subtasks(self):
        group_task = dict_to_task({
            'name': self.task.name + ':process',
            'actions': None
        })
        group_task.has_subtask = True
        for i, (infile, outfile) in enumerate(
                self.fanout.iter_in_out_files(self.task.pspace)):
            code = '''
from psyrun.worker import SerialWorker as Worker
Worker().start(task.execute, {infile!r}, {outfile!r})
            '''.format(infile=infile, outfile=outfile)

            t = dict_to_task({
                'name': '{0}:process:{1}'.format(self.task.name, i),
                'task_dep': [self.task.name + ':split'],
                'file_dep': [infile],
                'targets': [outfile],
                'getargs': {'depends_on': (self.task.name + ':split', 'id')},
                'actions': [(self._submit, [code])],
            })
            t.is_subtask = True
            group_task.task_dep.append(t.name)
            yield t
        yield group_task

    def create_merge_subtask(self):
        result_file = os.path.join(self.fanout.workdir, 'result.h5')
        code = '''
from psyrun.fanout import SingleItemFanOut as FanOut
FanOut({workdir!r}).merge({filename!r})
        '''.format(workdir=self.fanout.workdir, filename=result_file)

        file_deps = [f for _, f in self.fanout.iter_in_out_files(
            self.task.pspace)]
        return dict_to_task({
            'name': self.task.name + ':merge',
            'task_dep': ['{0}:process:{1}'.format(self.task.name, i)
                         for i in range(len(file_deps))],
            'file_dep': file_deps,
            'targets': [result_file],
            'getargs': {'depends_on': (self.task.name + ':process', 'id')},
            'actions': [(self._submit, [code])],
            })


def psydoit(taskdir, workdir, argv=sys.argv[1:]):
    return DoitMain(PackageLoader(taskdir, workdir)).run(argv)
