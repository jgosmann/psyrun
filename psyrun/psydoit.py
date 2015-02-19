import os
import os.path
import re
import sys

from doit.task import dict_to_task
from doit.cmd_base import TaskLoader
from doit.doit_cmd import DoitMain

from psyrun.split import Splitter
from psyrun.scheduler import ImmediateRun
from psyrun.worker import SerialWorker


class TaskDef(object):
    TASK_PATTERN = re.compile(r'^task_(.*)$')

    def __init__(self, path, conf=None):
        if conf is None:
            conf = Config()

        _set_public_attrs_from_dict(self, _load_pyfile(path))
        self.path = path

        if not hasattr(self, 'name'):
            prefixed_name, _ = os.path.splitext(os.path.basename(path))
            m = self.TASK_PATTERN.match(prefixed_name)
            if m:
                self.name = m.group(1)
            else:
                self.name = prefixed_name

        conf.apply_as_default(self)


def _load_pyfile(filename):
    with open(filename, 'r') as f:
        source = f.read()
    code = compile(source, filename, 'exec')
    loaded = {}
    exec(code, loaded)
    return loaded


def _set_public_attrs_from_dict(obj, d):
    for k, v in d.items():
        if not k.startswith('_'):
            setattr(obj, k, v)


class Config(object):
    __slots__ = ['workdir', 'worker', 'scheduler', 'scheduler_args', 'python']

    def __init__(self):
        self.workdir = 'psywork'
        self.worker = SerialWorker()
        self.scheduler = ImmediateRun()
        self.scheduler_args = None
        self.python = 'python'

    @classmethod
    def load_from_file(cls, filename):
        conf = cls()
        loaded_conf = _load_pyfile(filename)
        _set_public_attrs_from_dict(conf, loaded_conf)
        return conf

    def apply_as_default(self, task):
        for attr in self.__slots__:
            if not hasattr(task, attr):
                setattr(task, attr, getattr(self, attr))


class PackageLoader(TaskLoader):
    def __init__(self, taskdir, workdir):
        self.taskdir = taskdir
        self.workdir = workdir
        conffile = os.path.join(self.taskdir, 'psyconf.py')
        if os.path.exists(conffile):
            self.conf = Config.load_from_file(conffile)
        else:
            self.conf = Config()

    def load_tasks(self, cmd, opt_values, pos_args):
        task_list = []
        for filename in os.listdir(self.taskdir):
            root, ext = os.path.splitext(filename)
            if TaskDef.TASK_PATTERN.match(root) and ext == '.py':
                task = TaskDef(os.path.join(self.taskdir, filename), self.conf)
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
        self.splitter = Splitter(os.path.join(workdir, task.name), task.pspace)
        self.task = task

    def _submit(self, code, depends_on=None):
        code = '''
from psyrun.psydoit import TaskDef
task = TaskDef({path!r})
{code}
        '''.format(path=self.task.path, code=code)
        return {'id': self.task.scheduler.submit(
            [self.task.python, '-c', code], depends_on=depends_on,
            scheduler_args=self.task.scheduler_args)}

    def create_split_subtask(self):
        code = '''
from psyrun.split import Splitter
Splitter({workdir!r}, task.pspace).split()
        '''.format(workdir=self.splitter.workdir)

        return dict_to_task({
            'name': self.task.name + ':split',
            'file_dep': [self.task.path],
            'targets': [f for f, _ in self.splitter.iter_in_out_files()],
            'actions': [(self._submit, [code])],
        })

    def create_process_subtasks(self):
        group_task = dict_to_task({
            'name': self.task.name + ':process',
            'actions': None
        })
        group_task.has_subtask = True
        for i, (infile, outfile) in enumerate(
                self.splitter.iter_in_out_files()):
            code = '''
task.worker.start(task.execute, {infile!r}, {outfile!r})
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
        result_file = os.path.join(self.splitter.workdir, 'result.h5')
        code = '''
from psyrun.split import Splitter
Splitter.merge({workdir!r}, {filename!r})
        '''.format(workdir=self.splitter.workdir, filename=result_file)

        file_deps = [f for _, f in self.splitter.iter_in_out_files()]
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
