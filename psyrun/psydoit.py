import os
import os.path
import re
import sys

from doit.task import dict_to_task
from doit.cmd_base import TaskLoader
from doit.doit_cmd import DoitMain

from psyrun.processing import Splitter
from psyrun.scheduler import ImmediateRun
from psyrun.mapper import map_pspace
from psyrun.venv import init_virtualenv


class TaskDef(object):
    """Task defined by a Python file.

    Parameters
    ----------
    path : str
        Python file to load as task.
    conf : :class:`.Config`
        Default values for task parameters.
    """

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
    """Task configuration.


    Attributes
    ----------
    workdir : str
        Working directory to store task data in.
    result_file : str or None
        Path to save the results of the finished task at. If ``None``, this
        defaults to ``'result.h5'`` in the `workdir`.
    mapper : function
        Function to map the task's ``execute`` function onto the parameter
        space. Defaults to serial execution.
    mapper_kwargs : dict
        Additional keyword arguments for the `mapper` function.
    scheduler : :class:`.Scheduler`
        Scheduler to use to submit individual jobs. Defaults to
        :class:`.ImmediateRun`.
    scheduler_args : dict
        Additional scheduler arguments.
    python : str
        Path to Python interpreter to use.
    max_splits : int
        Maximum number of splits of the parameter space. This limits the number
        of jobs started.
    min_items : int
        Minimum number of parameter sets to evaluate per job.
    """

    __slots__ = [
        'workdir', 'result_file', 'mapper', 'mapper_kwargs', 'scheduler',
        'scheduler_args', 'python', 'max_splits', 'min_items']

    def __init__(self):
        self.workdir = 'psywork'
        self.result_file = None
        self.mapper = map_pspace
        self.mapper_kwargs = {}
        self.scheduler = ImmediateRun()
        self.scheduler_args = dict()
        self.python = sys.executable
        self.max_splits = 64
        self.min_items = 4

    @classmethod
    def load_from_file(cls, filename):
        """Load the config values from a Python file.

        Parameters
        ----------
        filename : str
            Python file to load.
        """
        conf = cls()
        loaded_conf = _load_pyfile(filename)
        _set_public_attrs_from_dict(conf, loaded_conf)
        return conf

    def apply_as_default(self, task):
        """Copies the attributes to a different object given they are not set
        in that object.

        Parameters
        ----------
        task : obj
            Object to copy the attributes to.
        """
        for attr in self.__slots__:
            if not hasattr(task, attr):
                setattr(task, attr, getattr(self, attr))


class PackageLoader(TaskLoader):
    """Loads doit tasks from Python files.

    Filenames have to match the regex defined in :const:`.TaskDef.TASK_PATTERN`.
    See :class:`.Config` for supported module level variables in the task
    definition.

    Parameters
    ----------
    taskdir : str
        Directory to load files from.
    """
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
        group_task = dict_to_task({
            'name': task.name,
            'actions': None
        })
        group_task.has_subtask = True

        creator = DistributeSubtaskCreator(task)
        for st in creator.create_subtasks():
            st.is_subtask = True
            group_task.task_dep.append(st.name)
            yield st
        yield group_task


class DistributeSubtaskCreator(object):
    """Create subtasks for to distribute parameter evaluations.

    Parameters
    ----------
    task : :class:`.TaskDef`
        Task definition to create subtasks for.
    """

    def __init__(self, task):
        self.splitter = Splitter(
            os.path.join(task.workdir, task.name), task.pspace,
            task.max_splits, task.min_items)
        self.task = task

    def _submit(self, code, name, depends_on=None):
        """Submits some code to execute to the task scheduler.

        Parameters
        ----------
        code : str
            Code to execute in job.
        name : str
            Job name.
        depends_on : sequence
            Job IDs that have to finish before the submitted code can be
            executed.

        Returns
        -------
        dict
            Contains the id of the submitted job under the key ``'id'``.
        """
        if depends_on is not None:
            try:
                depends_on = list(depends_on.values())
            except AttributeError:
                depends_on = [depends_on]
        code = '''
try:
    import faulthandler
    faulthandler.enable()
except:
    pass

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

    def create_subtasks(self):
        """Yields all the required subtasks."""
        yield self.create_split_subtask()
        for st in self.create_process_subtasks():
            yield st
        yield self.create_merge_subtask()

    def create_split_subtask(self):
        code = '''
from psyrun.processing import Splitter
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
from psyrun.processing import Worker
Worker(task.mapper, **task.mapper_kwargs).start(
    task.execute, {infile!r}, {outfile!r})
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
        if self.task.result_file:
            result_file = self.task.result_file
        else:
            result_file = os.path.join(self.splitter.workdir, 'result.h5')
        code = '''
from psyrun.processing import Splitter
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
    """Runs psy-doit tasks.

    Parameters
    ----------
    taskdir : str
        Path to directory with task definition files.
    argv : sequence of str
        psy-doit command line arguments.

    Returns
    -------
    int
        Return code. See the `doit documentation
        <http://pydoit.org/api/doit.doit_cmd.DoitMain-class.html#run>`_ for
        more details.
    """
    init_virtualenv()
    return DoitMain(PackageLoader(taskdir)).run(argv)
