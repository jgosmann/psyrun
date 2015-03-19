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
from psyrun.venv import init_virtualenv


class TaskDef(object):
    TASK_PATTERN = re.compile(r'^task_(.*)$')

    def __init__(self, path, conf=None):
        if conf is None:
            conf = Config()

        _set_public_attrs_from_dict(
            self, _load_pyfile(path), only_existing=False)
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
    source = '''
import sys
sys.path = {path!r}
sys.path.insert(0, {taskdir!r})
'''.format(path=sys.path, taskdir=os.path.dirname(filename))
    with open(filename, 'r') as f:
        source += f.read()
    code = compile(source, filename, 'exec')
    loaded = {}
    exec(code, loaded)
    return loaded


def _set_public_attrs_from_dict(obj, d, only_existing=True):
    for k, v in d.items():
        if not k.startswith('_') and (not only_existing or hasattr(obj, k)):
            setattr(obj, k, v)


class Config(object):
    __slots__ = [
        'workdir', 'worker', 'scheduler', 'scheduler_args', 'python',
        'max_splits', 'min_items']

    def __init__(self):
        self.workdir = 'psywork'
        self.worker = SerialWorker()
        self.scheduler = ImmediateRun()
        self.scheduler_args = dict()
        self.python = sys.executable
        self.max_splits = 64
        self.min_items = 4

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
    def __init__(self, taskdir):
        self.taskdir = taskdir
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
        creator = FanOutSubtaskCreator(task)
        yield creator.create_split_subtask()
        for st in creator.create_process_subtasks():
            yield st
        yield creator.create_merge_subtask()


class FanOutSubtaskCreator(object):
    def __init__(self, task):
        self.splitter = Splitter(
            os.path.join(task.workdir, task.name), task.pspace,
            task.max_splits, task.min_items)
        self.task = task

    def _submit(self, code, name, depends_on=None):
        if depends_on is not None:
            try:
                depends_on = list(depends_on.values())
            except AttributeError:
                depends_on = [depends_on]
        code = '''
import sys
sys.path = {path!r}
sys.path.insert(0, {taskdir!r})

from psyrun.psydoit import TaskDef
task = TaskDef({taskpath!r})
{code}
        '''.format(
            path=sys.path, taskdir=os.path.dirname(self.task.path),
            taskpath=self.task.path, code=code)
        codefile = os.path.join(self.splitter.workdir, name + '.py')
        scheduler_args = dict(self.task.scheduler_args)
        scheduler_args.update({
            'output_file': os.path.join(self.splitter.workdir, name + '.log'),
            'depends_on': depends_on,
            'name': name
        })
        with open(codefile, 'w') as f:
            f.write(code)
        return {'id': self.task.scheduler.submit(
            [self.task.python, codefile], scheduler_args)}

    def create_split_subtask(self):
        code = '''
from psyrun.split import Splitter
Splitter({workdir!r}, task.pspace, {max_splits!r}, {min_items!r}).split()
        '''.format(
            workdir=self.splitter.workdir, max_splits=self.task.max_splits,
            min_items=self.task.min_items)

        name = self.task.name + ':split'
        return dict_to_task({
            'name': name,
            'file_dep': [self.task.path],
            'actions': [(self._submit, [code, name])],
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

            name = '{0}:process:{1}'.format(self.task.name, i)
            t = dict_to_task({
                'name': name,
                'task_dep': [self.task.name + ':split'],
                'getargs': {'depends_on': (self.task.name + ':split', 'id')},
                'actions': [(self._submit, [code, name])],
            })
            t.is_subtask = True
            group_task.task_dep.append(t.name)
            yield t
        yield group_task

    def create_merge_subtask(self):
        result_file = os.path.join(self.splitter.workdir, 'result.h5')
        code = '''
from psyrun.split import Splitter
Splitter.merge({outdir!r}, {filename!r})
        '''.format(outdir=self.splitter.outdir, filename=result_file)

        name = self.task.name + ':merge'
        file_deps = [f for _, f in self.splitter.iter_in_out_files()]
        return dict_to_task({
            'name': name,
            'task_dep': ['{0}:process:{1}'.format(self.task.name, i)
                         for i in range(len(file_deps))],
            'getargs': {'depends_on': (self.task.name + ':process', 'id')},
            'actions': [(self._submit, [code, name])],
            })


def psydoit(taskdir, argv=sys.argv[1:]):
    init_virtualenv()
    return DoitMain(PackageLoader(taskdir)).run(argv)
