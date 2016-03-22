import itertools
import os
import os.path
import re
import sys
import traceback
import warnings

from doit.task import dict_to_task
from doit.cmd_base import TaskLoader
from doit.doit_cmd import DoitMain

from psyrun.io import NpzStore
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
    source = ''
    with open(filename, 'r') as f:
        source += f.read()
    code = compile(source, filename, 'exec')
    loaded = {'__file__': filename}
    exec(code, loaded)  # pylint: disable=exec-used
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
        defaults to ``'result.<ext>'`` in the `workdir`.
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
    file_dep : list of str
        Additional files the task depends on.
    io : :class:`DictStore`
        Input/output backend. Defaults to :class:`NpzStore`.
    """

    __slots__ = [
        'workdir', 'result_file', 'mapper', 'mapper_kwargs', 'scheduler',
        'scheduler_args', 'python', 'max_splits', 'min_items', 'file_dep',
        'io']

    def __init__(self):
        self.workdir = os.path.abspath('psywork')
        self.result_file = None
        self.mapper = map_pspace
        self.mapper_kwargs = {}
        self.scheduler = ImmediateRun()
        self.scheduler_args = dict()
        self.python = sys.executable
        self.max_splits = 64
        self.min_items = 4
        self.file_dep = []
        self.io = NpzStore()

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

    Filenames have to match the regex defined in
    :const:`.TaskDef.TASK_PATTERN`.  See :class:`.Config` for supported module
    level variables in the task definition.

    Parameters
    ----------
    taskdir : str
        Directory to load files from.
    """
    def __init__(self, taskdir):
        super(PackageLoader, self).__init__()
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
                path = os.path.join(self.taskdir, filename)
                try:
                    task = TaskDef(path, self.conf)
                    task_list.extend(self.create_task(task))
                except Exception:
                    traceback.print_exc()
                    warnings.warn("Task {path!r} could not be loaded.".format(
                        path=path))
        return task_list, {}

    def create_task(self, task):
        group_task = dict_to_task({
            'name': task.name,
            'actions': None
        })
        group_task.has_subtask = True

        creator = DistributeSubtaskCreator(task)
        return creator.create_subtasks()


class Job(object):
    def __init__(self, name, submit_fn, code, dependencies, targets):
        self.name = name
        self.submit_fn = submit_fn
        self.code = code
        self.dependencies = dependencies
        self.targets = targets


class JobChain(object):
    def __init__(self, name, jobs):
        self.name = name
        self.jobs = jobs

    @property
    def dependencies(self):
        return self.jobs[0].dependencies

    @property
    def targets(self):
        return self.jobs[-1].targets


class JobGroup(object):
    def __init__(self, name, jobs):
        self.name = name
        self.jobs = jobs

    @property
    def dependencies(self):
        return itertools.chain(j.dependencies for j in self.jobs)

    @property
    def targets(self):
        return itertools.chain.from_iterable(j.targets for j in self.jobs)


class JobTreeVisitor(object):
    def __init__(self):
        self._dispatcher = {
            Job: self.visit_job,
            JobChain: self.visit_chain,
            JobGroup: self.visit_group
        }

    def visit_job(self, job):
        raise NotImplementedError()

    def visit_chain(self, chain):
        raise NotImplementedError()

    def visit_group(self, group):
        raise NotImplementedError()

    def visit(self, job):
        return self._dispatcher[job.__class__](job)


class ToDoitTask(JobTreeVisitor):
    def __init__(self, names, uptodate):
        super(ToDoitTask, self).__init__()
        self.names = names
        self.uptodate = uptodate
        self.task_dep = []
        self.getargs = {}

    def visit_job(self, job):
        yield dict_to_task({
            'name': self.names[job],
            'uptodate': [lambda: self.uptodate[job]],
            'task_dep': self.task_dep,
            'getargs': self.getargs,
            'actions': [(job.submit_fn, [job.code, self.names[job]])],
        })

    def visit_group(self, group):
        subtasks = itertools.chain.from_iterable(
            self.visit(job) for job in group.jobs)

        task_dict = {
            'name': self.names[group],
            'uptodate': [lambda: self.uptodate[group]],
            'task_dep': [self.names[job] for job in group.jobs],
            'actions': [],
        }

        task = dict_to_task(task_dict)
        task.has_subtask = True
        yield task
        for task in subtasks:
            task.is_subtask = True
            yield task

    def visit_chain(self, chain):
        subtasks = []
        old_task_dep = self.task_dep
        old_getargs = self.getargs
        for job in chain.jobs:
            subtasks.extend(self.visit(job))
            self.task_dep = old_task_dep + [self.names[job]]
            self.getargs = {'depends_on': (self.names[job], 'id')}
        self.task_dep = old_task_dep
        self.getargs = old_getargs

        task_dict = {
            'name': self.names[chain],
            'uptodate': [lambda: self.uptodate[chain]],
            'task_dep': [self.names[job] for job in chain.jobs],
            'actions': [],
        }

        task = dict_to_task(task_dict)
        task.has_subtask = True
        yield task
        for task in subtasks:
            task.is_subtask = True
            yield task


class Fullname(JobTreeVisitor):
    def __init__(self, jobtree):
        super(Fullname, self).__init__()
        self.prefix = ''
        self.names = {}
        self.visit(jobtree)

    def visit_job(self, job):
        self.names[job] = self.prefix + job.name

    def visit_chain(self, chain):
        self.visit_group(chain)

    def visit_group(self, group):
        self.names[group] = self.prefix + group.name
        old_prefix = self.prefix
        self.prefix += group.name + ':'
        for job in group.jobs:
            self.visit(job)
        self.prefix = old_prefix


class Uptodate(JobTreeVisitor):
    def __init__(self, jobtree, names, scheduler):
        super(Uptodate, self).__init__()
        self.names = names
        self.scheduler = scheduler
        self.status = {}
        self.clamp = None
        self.visit(jobtree)

    def visit_job(self, job):
        if self.is_job_queued(job):
            self.status[job] = True
        elif self.clamp is None:
            tref = self._get_tref(job.dependencies)
            self.status[job] = self.files_uptodate(tref, job.targets)
        else:
            self.status[job] = self.clamp
        return self.status[job]

    def visit_chain(self, chain):
        if self.clamp is None:
            tref = self._get_tref(chain.jobs[0].dependencies)

            last_uptodate = -1
            for i, job in enumerate(reversed(chain.jobs)):
                if self.files_uptodate(tref, job.targets):
                    last_uptodate = len(chain.jobs) - i - 1
                    break

            for i, job in enumerate(chain.jobs):
                if i <= last_uptodate:
                    self.clamp = True
                elif i == last_uptodate + 1:
                    self.clamp = None
                else:
                    self.clamp = False
                self.visit(job)

            self.status[chain] = last_uptodate + 1 == len(chain.jobs)
            self.clamp = None
        else:
            for job in chain.jobs:
                self.visit(job)
            self.status[chain] = self.clamp
        return self.status[chain]

    def visit_group(self, group):
        subtask_status = [self.visit(j) for j in group.jobs]
        self.status[group] = all(subtask_status)
        return self.status[group]

    def is_job_queued(self, job):
        job_names = [
            self.scheduler.get_status(j).name
            for j in self.scheduler.get_jobs()]
        return self.names[job] in job_names

    def files_uptodate(self, tref, targets):
        return all(
            self._is_newer_than_tref(target, tref) for target in targets)

    def _get_tref(self, dependencies):
        tref = 0
        deps = [d for d in dependencies if os.path.exists(d)]
        if len(deps) > 0:
            tref = max(os.stat(d).st_mtime for d in deps)
        return tref

    def _is_newer_than_tref(self, filename, tref):
        return os.path.exists(filename) and os.stat(filename).st_mtime >= tref


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
            task.max_splits, task.min_items, io=task.io)
        self.task = task

    @property
    def result_file(self):
        if self.task.result_file:
            return self.task.result_file
        else:
            return os.path.join(
                self.splitter.workdir, 'result' + self.splitter.io.ext)

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

import os
os.chdir({taskdir!r})

from psyrun.psydoit import TaskDef
task = TaskDef({taskpath!r})
{code}
        '''.format(
            path=sys.path,
            taskdir=os.path.abspath(os.path.dirname(self.task.path)),
            taskpath=os.path.abspath(self.task.path), code=code)
        codefile = os.path.join(self.splitter.workdir, name + '.py')
        output_filename = os.path.join(self.splitter.workdir, name + '.log')
        with open(codefile, 'w') as f:
            f.write(code)

        for job in self.task.scheduler.get_jobs():
            status = self.task.scheduler.get_status(job)
            if status is not None and name == status.name:
                self.task.scheduler.kill(job)

        return {'id': self.task.scheduler.submit(
            [self.task.python, codefile], output_filename, name, depends_on,
            self.task.scheduler_args)}

    def create_subtasks(self):
        job = self.create_job()
        names = Fullname(job).names
        return ToDoitTask(names, Uptodate(
            job, names, self.task.scheduler).status).visit(job)

    def create_job(self):
        split = self.create_split_job()
        process = self.create_process_job()
        merge = self.create_merge_job()
        return JobChain(self.task.name, [split, process, merge])

    def create_split_job(self):
        code = '''
from psyrun.processing import Splitter
Splitter(
    {workdir!r}, task.pspace, {max_splits!r}, {min_items!r},
    io=task.io).split()
        '''.format(
            workdir=self.splitter.workdir, max_splits=self.task.max_splits,
            min_items=self.task.min_items)
        file_dep = [os.path.join(os.path.dirname(self.task.path), f)
                    for f in self.task.file_dep]
        return Job(
            'split', self._submit, code, [self.task.path] + file_dep,
            [f for f, _ in self.splitter.iter_in_out_files()])

    def create_process_job(self):
        jobs = []
        for i, (infile, outfile) in enumerate(
                self.splitter.iter_in_out_files()):
            code = '''
from psyrun.processing import Worker
execute = task.execute
Worker(task.mapper, io=task.io, **task.mapper_kwargs).start(
    execute, {infile!r}, {outfile!r})
            '''.format(infile=infile, outfile=outfile)
            jobs.append(Job(str(i), self._submit, code, [infile], [outfile]))

        group = JobGroup('process', jobs)
        return group

    def create_merge_job(self):
        code = '''
from psyrun.processing import Splitter
Splitter.merge({outdir!r}, {filename!r}, append=False, io=task.io)
        '''.format(outdir=self.splitter.outdir, filename=self.result_file)
        return Job(
            'merge', self._submit, code,
            [f for _, f in self.splitter.iter_in_out_files()],
            [self.result_file])


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
