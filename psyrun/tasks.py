"""Psyrun's task handling API."""

from __future__ import print_function

import itertools
import os
import os.path
import re
import sys
import traceback
import warnings

from psyrun.store import PickleStore
from psyrun.pspace import missing, Param
from psyrun.processing import Splitter
from psyrun.scheduler import ImmediateRun
from psyrun.utils.doc import inherit_docs


class TaskWarning(UserWarning):
    """General warning related to task processing.

    *TaskWarnings* will always be shown by default.
    """
    pass


class JobsRunningWarning(TaskWarning):
    """Warning issued when jobs for a task are still running."""
    def __init__(self, name):
        self.task = name
        super(JobsRunningWarning, self).__init__(
            "Task '{}' has unfinished jobs queued. Not starting new jobs "
            "until these are finished or have been killed.".format(name))


class TaskWorkdirDirtyWarning(TaskWarning):
    """Warning issued when the workdir is dirty and would be overwritten."""
    def __init__(self, name):
        # TODO explain how to solve this
        self.task = name
        super(TaskWorkdirDirtyWarning, self).__init__(
            "Work directory of task '{}' is dirty.".format(name))


warnings.simplefilter('always', category=TaskWarning)


class TaskDef(object):
    """Task defined by a Python file.

    Parameters
    ---------
    path : `str`
        Python file to load as task.
    conf : `Config`
        Default values for task parameters.

    Attributes
    ----------
    TASK_PATTERN : `re.RegexObject`
        Regular expression to match task filenames.
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
    backend : `Backend`, default: `DistributeBackend`
        The processing backend which determines how work is distributed across
        jobs.
    file_dep : sequence of `str`, default: ``[]``
        Additional files the task depends on.
    max_jobs : `int`, default: ``100``
        Maximum number of jobs to start. With less jobs each job has to process
        more parameter assignments. It depends on the  scheduler and backend
        used to which degree these will run in parallel.
    min_items : `int`, default: ``1``
        Minimum number of parameter assignment to evaluate per job. If a single
        assignment is fast to evaluate, increasing this number can improve
        performance because Psyrun will not start a new job for each parameter
        assignment which can save some overhead.
    overwrite_dirty : bool, default: `True`
        Whether to overwrite dirty workdirs without a warning.
    pspace : `ParameterSpace`, required
        Parameter space to evaluate.
    python : `str`, default: ``sys.executable``
        Path to Python interpreter to use.
    resultfile : `str` or `None`, default: `None`
        Path to save the results of the finished task at. If ``None``, this
        defaults to ``'result.<ext>'`` in the *workdir*.
    scheduler : `Scheduler`, default: `ImmediateRun`
        Scheduler to use to submit individual jobs.
    scheduler_args : `dict`, default: ``{}``
        Additional scheduler arguments. See the documentation of the
        scheduler for details.
    store : `Store`, default: `PickleStore`
        Input/output backend.
    workdir : `str`, default: ``'psy-work'``
        Working directory to store results and supporting data to process the
        task.

    """

    __slots__ = [
        'backend', 'file_dep', 'max_jobs', 'min_items', 'pspace',
        'overwrite_dirty', 'python', 'resultfile', 'scheduler',
        'scheduler_args', 'store', 'workdir']

    def __init__(self):
        self.backend = DistributeBackend
        self.file_dep = []
        self.max_jobs = 100
        self.min_items = 1
        self.overwrite_dirty = True
        self.pspace = Param()
        self.python = sys.executable
        self.resultfile = None
        self.scheduler = ImmediateRun()
        self.scheduler_args = dict()
        self.store = PickleStore()
        self.workdir = os.path.abspath('psy-work')

    @classmethod
    def load_from_file(cls, filename):
        """Load the configuration values from a Python file.

        Parameters
        ----------
        filename : `str`
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


class PackageLoader(object):
    """Loads tasks from Python files.

    Filenames have to match the regular expression defined in
    `TaskDef.TASK_PATTERN`. See `Config` for supported module
    level variables in the task definition.

    It is possible to set these variables for all tasks by setting them in
    the file ``psy-conf.py`` in the *taskdir*.

    Parameters
    ----------
    taskdir : `str`
        Directory to load task files from.

    Attributes
    ----------
    taskdir : `str`
        Directory to load task files from.
    conf : `Config`
        Default values for module level task variables.
    """
    def __init__(self, taskdir):
        super(PackageLoader, self).__init__()
        self.taskdir = taskdir
        conffile = os.path.join(self.taskdir, 'psy-conf.py')
        if os.path.exists(conffile):
            self.conf = Config.load_from_file(conffile)
        else:
            self.conf = Config()

    def load_task_defs(self):
        """Load task definitions.

        Returns
        -------
        `list` of `TaskDef`
            Task definitions.
        """
        task_defs = []
        for filename in os.listdir(self.taskdir):
            root, ext = os.path.splitext(filename)
            if TaskDef.TASK_PATTERN.match(root) and ext == '.py':
                path = os.path.join(self.taskdir, filename)
                try:
                    task_defs.append(TaskDef(path, self.conf))
                except Exception:
                    traceback.print_exc()
                    warnings.warn("Task {path!r} could not be loaded.".format(
                        path=path))
        return task_defs


class Job(object):
    """Describes a single processing job.

    Parameters
    ----------
    name : `str`
        Name of the job.
    submit_fn : function
        Function to use to submit the job for processing.
    code : `str`
        Python code to execute.
    dependencies : sequence
        Identifiers of other jobs that need to finish first before this job
        can be run.
    targets : sequence of `str`
        Files created by this job.

    Attributes
    ----------
    name : `str`
        Name of the job.
    submit_fn : function
        Function to use to submit the job for processing.
    code : `str`
        Python code to execute.
    dependencies : sequence
        Identifiers of other jobs that need to finish first before this job
        can be run.
    targets : sequence of `str`
        Files created by this job.
    """
    def __init__(self, name, submit_fn, code, dependencies, targets):
        self.name = name
        self.submit_fn = submit_fn
        self.code = code
        self.dependencies = dependencies
        self.targets = targets


class JobChain(object):
    """Chain of jobs to run in succession.

    Parameters
    ----------
    name : `str`
        Name of the job chain.
    jobs : sequence of `Job`
        Jobs to run in succession.

    Attributes
    ----------
    name : `str`
        Name of the job chain.
    jobs : sequence of `Job`
        Jobs to run in succession.
    dependencies : sequence
        Jobs that need to run first before the job chain can be run (equivalent
        to the dependencies of the first job in the chain).
    targets : sequence of `str`
        Files created or updated by the job chain (equivalent to the targets
        of the last job in the chain).
    """
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
    """Group of jobs that can run in parallel.

    Parameters
    ----------
    name : `str`
        Name of the job group.
    jobs : sequence of `Job`
        Jobs to run in the job group.

    Attributes
    ----------
    name : `str`
        Name of the job group.
    jobs : sequence of `Job`
        Jobs to run in the job group.
    dependencies : sequence
        Jobs that need to run first before the job group can be run (equivalent
        to the union of all the group's job's dependencies).
    targets : sequence of `str`
        Files that will be created or updated by the group's jobs (equivalent
        to the union of all the group's job's targets).
    """
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
    """Abstract base class to implement visitors on trees of jobs.

    Base class to implement visitors following the Visitor pattern to traverse
    the tree constructed out of `Job`, `JobChain`, and `JobGroup` instances.

    A deriving class should overwrite `visit_job`, `visit_chain`, and
    `visit_group`. Use the `visit` method to start visiting a tree of jobs.
    """

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
        """Visit all jobs in the tree *job*."""
        return self._dispatcher[job.__class__](job)


@inherit_docs
class Submit(JobTreeVisitor):
    """Submit all jobs that are not up-to-date.

    The constructor will call `visit`.

    Parameters
    ----------
    job : job tree
        Tree of jobs to submit.
    names : `dict`
        Maps jobs to their names. (Can be obtained with `Fullname`.)
    uptodate : `dict`
        Maps jobs to their up-to-date status.
        (Can be obtained with `Uptodate`.)

    Attributes
    ----------
    names : `dict`
        Maps jobs to their names.
    uptodate : `dict`
        Maps jobs to their up-to-date status.
    """
    def __init__(self, job, names, uptodate):
        super(Submit, self).__init__()
        self.names = names
        self.uptodate = uptodate
        self._depends_on = []
        self.visit(job)

    def visit_job(self, job):
        if self.uptodate.status[job]:
            print('-', self.names[job])
            return []
        else:
            print('.', self.names[job])
            return [job.submit_fn(
                job.code, self.names[job], depends_on=self._depends_on)]

    def visit_group(self, group):
        return sum((self.visit(job) for job in group.jobs), [])

    def visit_chain(self, chain):
        old_depends_on = self._depends_on
        job_ids = []
        for job in chain.jobs:
            ids = self.visit(job)
            job_ids.extend(ids)
            self._depends_on = old_depends_on + ids
        self._depends_on = old_depends_on
        return job_ids


@inherit_docs
class Clean(JobTreeVisitor):
    """Clean all target files and supporting files of jobs that are outdated.

    The constructor will call `visit`.

    Parameters
    ----------
    job : job tree
        Tree of jobs to clean.
    task : `TaskDef`
        Task that generated the job tree.
    names : `dict`
        Maps jobs to their names. (Can be obtained with `Fullname`.)
    uptodate : `dict`, optional
        Maps jobs to their up-to-date status.
        (Can be obtained with `Uptodate`.)
        If not provided, all jobs are treated as outdated.

    Attributes
    ----------
    task : `TaskDef`
        Task that generated the job tree.
    names : `dict`
        Maps jobs to their names.
    uptodate : `dict`
        Maps jobs to their up-to-date status.
    """
    def __init__(self, job, task, names, uptodate=None):
        super(Clean, self).__init__()
        self.task = task
        self.names = names
        if uptodate is None:
            uptodate = {}
        self.uptodate = uptodate
        self.visit(job)

    def visit_job(self, job):
        if self.uptodate.get(job, False):
            return

        workdir = os.path.join(self.task.workdir, self.task.name)
        for item in os.listdir(workdir):
            if item.startswith(self.names[job]):
                os.remove(os.path.join(workdir, item))
        for t in job.targets:
            if os.path.exists(t):
                os.remove(t)

    def visit_chain(self, chain):
        pass

    def visit_group(self, chain):
        pass


@inherit_docs
class Fullname(JobTreeVisitor):
    """Construct names of the jobs.

    The constructor will call `visit`.

    Parameters
    ----------
    jobtree : job tree
        Tree of jobs to construct names for.

    Attributes
    ----------
    names : `dict`
        Maps jobs to their names.
    """
    def __init__(self, jobtree):
        super(Fullname, self).__init__()
        self._prefix = ''
        self.names = {}
        self.visit(jobtree)

    def visit_job(self, job):
        self.names[job] = self._prefix + job.name

    def visit_chain(self, chain):
        self.visit_group(chain)

    def visit_group(self, group):
        self.names[group] = self._prefix + group.name
        old_prefix = self._prefix
        self._prefix += group.name + ':'
        for job in group.jobs:
            self.visit(job)
        self._prefix = old_prefix


@inherit_docs
class Uptodate(JobTreeVisitor):
    """Determines the up-to-date status of jobs.

    The constructor will call `visit`.

    Parameters
    ----------
    jobtree : job tree
        Tree of jobs to determine the up-to-date status for.
    names : `dict`
        Maps jobs to their names. (Can be obtained with `Fullname`.)
    task : `TaskDef`
        Task that generated the job tree.

    Attributes
    ----------
    names : `dict`
        Maps jobs to their names.
    task : `TaskDef`
        Task that generated the job tree.
    status : `dict`
        Maps jobs to their up-to-date status.
    """
    def __init__(self, jobtree, names, task):
        super(Uptodate, self).__init__()
        self.names = names
        self.task = task
        self.status = {}
        self._clamp = None

        self.any_queued = False
        self.outdated = False

        self.visit(jobtree)
        self.post_visit()

    def post_visit(self):
        """Called after `visit`.

        Checks whether jobs are still running and marks these as up-to-date
        while issuing a warning.
        """
        skip = False
        if self.any_queued and self.outdated:
            skip = True
            warnings.warn(JobsRunningWarning(self.task.name))
        if skip:
            for k in self.status:
                self.status[k] = True

    def visit_job(self, job):
        if self.is_job_queued(job):
            self.status[job] = True
        elif self._clamp is None:
            tref = self._get_tref(job.dependencies)
            self.status[job] = self.files_uptodate(tref, job.targets)
        else:
            self.status[job] = self._clamp
        return self.status[job]

    def visit_chain(self, chain):
        if self._clamp is None:
            tref = self._get_tref(chain.jobs[0].dependencies)

            last_uptodate = -1
            for i, job in enumerate(reversed(chain.jobs)):
                if self.files_uptodate(tref, job.targets):
                    last_uptodate = len(chain.jobs) - i - 1
                    break

            for i, job in enumerate(chain.jobs):
                if i <= last_uptodate:
                    self._clamp = True
                elif i == last_uptodate + 1:
                    self._clamp = None
                else:
                    self._clamp = False
                self.visit(job)

            self.status[chain] = last_uptodate + 1 == len(chain.jobs)
            self._clamp = None
        else:
            for job in chain.jobs:
                self.visit(job)
            self.status[chain] = self._clamp
        return self.status[chain]

    def visit_group(self, group):
        subtask_status = [self.visit(j) for j in group.jobs]
        self.status[group] = all(subtask_status)
        return self.status[group]

    def is_job_queued(self, job):
        """Checks whether *job* is queud."""
        job_names = [
            self.task.scheduler.get_status(j).name
            for j in self.task.scheduler.get_jobs()]
        is_queued = self.names[job] in job_names
        self.any_queued |= is_queued
        return is_queued

    def files_uptodate(self, tref, targets):
        """Checks that all *targets* are newer than *tref*."""
        uptodate = all(
            self._is_newer_than_tref(target, tref) for target in targets)
        self.outdated |= not uptodate
        return uptodate

    def _get_tref(self, dependencies):
        tref = 0
        deps = [d for d in dependencies if os.path.exists(d)]
        if len(deps) > 0:
            tref = max(os.stat(d).st_mtime for d in deps)
        return tref

    def _is_newer_than_tref(self, filename, tref):
        return os.path.exists(filename) and os.stat(filename).st_mtime >= tref


class Backend(object):
    """Abstract base class for processing backends.

    Processing backends determine how work is split across jobs.

    Deriving classes are supposed to implement `create_job` and `get_missing`.

    Parameters
    ----------
    task : `TaskDef`
        The task to create processing jobs for.

    Attributes
    ----------
    task : `TaskDef`
        The task to create processing jobs for.
    workdir : `str`
        Directory in which supporting files for processing the task are stored.
    """

    def __init__(self, task):
        super(Backend, self).__init__()
        self.task = task
        self.workdir = os.path.join(task.workdir, task.name)
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

    def submit(self, code, name, depends_on=None):
        """Submits some code to execute to the task scheduler.

        Parameters
        ----------
        code: `str`
            Code to execute in job.
        name: `str`
            Job name.
        depends_on: sequence
            Job IDs that have to finish before the submitted code can be
            executed.

        Returns
        -------
        `dict`
            Contains the id of the submitted job under the key ``'id'``.
        """
        code = '''
try:
    import faulthandler
    faulthandler.enable()
except:
    pass

import os
os.chdir({taskdir!r})

from psyrun.tasks import TaskDef
task = TaskDef({taskpath!r})
{code}
        '''.format(
            path=sys.path,
            taskdir=os.path.abspath(os.path.dirname(self.task.path)),
            taskpath=os.path.abspath(self.task.path), code=code)
        codefile = os.path.join(self.workdir, name + '.py')
        output_filename = os.path.join(self.workdir, name + '.log')
        with open(codefile, 'w') as f:
            f.write(code)

        for job in self.task.scheduler.get_jobs():
            status = self.task.scheduler.get_status(job)
            if status is not None and name == status.name:
                self.task.scheduler.kill(job)

        return self.task.scheduler.submit(
            [self.task.python, codefile], output_filename, name, depends_on,
            self.task.scheduler_args)

    def create_job(self, cont=False):
        """Create the job tree to process given task.

        Parameters
        ----------
        cont : `bool`, optional
            By default old results will be discarded, but when this option is
            set to `True`, old results will be kept and merged with the new
            results.
        """
        raise NotImplementedError()

    def get_missing(self):
        """Returns a `ParameterSpace` with missing parameter assignments.

        Missing paramaters assignments are parameter assignments requested by
        the task definition, but that have not been evaluated yet.
        """
        raise NotImplementedError


@inherit_docs
class DistributeBackend(Backend):
    """Create subtasks for distributed parameter evaluation.

    This will create one tasks that splits the parameter space in a number of
    equal batches (at most *max_jobs*, but with at least *min_items* for each
    batch). After processing all batches the results will be merged into a
    single file.

    Parameters
    ----------
    task : `TaskDef`
        Task definition to create subtasks for.
    """

    @property
    def resultfile(self):
        """File in which the results will be stored."""
        if self.task.resultfile:
            return self.task.resultfile
        else:
            return os.path.join(
                self.workdir, 'result' + self.task.store.ext)

    @property
    def pspace_file(self):
        """File that will store the input parameters space."""
        return os.path.join(self.workdir, 'pspace' + self.task.store.ext)

    def _try_mv_to_out(self, filename):
        try:
            os.rename(
                os.path.join(self.workdir, filename),
                os.path.join(self.workdir, 'out', filename))
        except OSError:
            pass

    def create_job(self, cont=False):
        if cont:
            outdir = os.path.join(self.workdir, 'out')
            self._try_mv_to_out('result' + self.task.store.ext)
            Splitter.merge(
                outdir, os.path.join(outdir, 'pre' + self.task.store.ext))
            for filename in os.listdir(outdir):
                if not filename.startswith('pre'):
                    os.remove(os.path.join(outdir, filename))
            pspace = self.get_missing()
        else:
            pspace = self.task.pspace

        self.task.store.save(self.pspace_file, pspace.build())
        splitter = Splitter(
            self.workdir, pspace, self.task.max_jobs, self.task.min_items,
            store=self.task.store)

        split = self.create_split_job(splitter)
        process = self.create_process_job(splitter)
        merge = self.create_merge_job(splitter)
        return JobChain(self.task.name, [split, process, merge])

    def create_split_job(self, splitter):
        code = '''
from psyrun.processing import Splitter
from psyrun.pspace import Param
pspace = Param(**task.store.load({pspace!r}))
Splitter(
    {workdir!r}, pspace, {max_jobs!r}, {min_items!r},
    store=task.store).split()
        '''.format(
            pspace=self.pspace_file,
            workdir=splitter.workdir, max_jobs=self.task.max_jobs,
            min_items=self.task.min_items)
        file_dep = [os.path.join(os.path.dirname(self.task.path), f)
                    for f in self.task.file_dep]
        return Job(
            'split', self.submit, code, [self.task.path] + file_dep,
            [f for f, _ in splitter.iter_in_out_files()])

    def create_process_job(self, splitter):
        jobs = []
        for i, (infile, outfile) in enumerate(
                splitter.iter_in_out_files()):
            code = '''
from psyrun.processing import Worker
execute = task.execute
Worker(store=task.store).start(execute, {infile!r}, {outfile!r})
            '''.format(infile=infile, outfile=outfile)
            jobs.append(Job(str(i), self.submit, code, [infile], [outfile]))

        group = JobGroup('process', jobs)
        return group

    def create_merge_job(self, splitter):
        code = '''
from psyrun.processing import Splitter
Splitter.merge({outdir!r}, {filename!r}, append=False, store=task.store)
        '''.format(outdir=splitter.outdir, filename=self.resultfile)
        return Job(
            'merge', self.submit, code,
            [f for _, f in splitter.iter_in_out_files()], [self.resultfile])

    def get_missing(self):
        pspace = self.task.pspace
        try:
            missing_items = missing(
                pspace, Param(**self.task.store.load(self.resultfile)))
        except IOError:
            missing_items = pspace
            for filename in os.path.join(self.workdir, 'out'):
                outfile = os.path.join(self.workdir, 'out', filename)
                try:
                    missing_items = missing(
                        missing_items,
                        Param(**self.task.store.load(outfile)))
                except IOError:
                    pass
        return missing_items


@inherit_docs
class LoadBalancingBackend(Backend):
    """Create subtasks for load balanced parameter evaluation.

    This will create *max_jobs* worker jobs that will fetch parameter
    assignments from a queue. Thus, all worker jobs should be busy all of the
    time.

    This backend is useful if individual parameters assignments can vary to a
    large degree in their processing time. It is recommended to use a `Store`
    that supports efficient fetching of a single row from a file.

    Parameters
    ----------
    task : `TaskDef`
        Task definition to create subtasks for.
    """

    @property
    def infile(self):
        """File from which input parameter assignments are fetched."""
        return os.path.join(self.workdir, 'in' + self.task.store.ext)

    @property
    def statusfile(self):
        """File that stores the processing status."""
        return os.path.join(self.workdir, 'status')

    @property
    def partial_resultfile(self):
        """File results are appended to while processing is in progress."""
        root, ext = os.path.splitext(self.resultfile)
        return root + '.part' + ext

    @property
    def resultfile(self):
        """Final result file."""
        if self.task.resultfile:
            return self.task.resultfile
        else:
            return os.path.join(self.workdir, 'result' + self.task.store.ext)

    def create_job(self, cont=False):
        pspace = self.create_pspace_job(cont=cont)
        process = self.create_process_job()
        finalize = self.create_finalize_job()
        return JobChain(self.task.name, [pspace, process, finalize])

    def create_pspace_job(self, cont):
        code = '''
import os.path
from psyrun.pspace import missing, Param
from psyrun.processing import LoadBalancingWorker
pspace = task.pspace
if {cont!r}:
    if os.path.exists({outfile!r}):
        pspace = missing(pspace, Param(**task.store.load({outfile!r})))
    if os.path.exists({part_outfile!r}):
        pspace = missing(pspace, Param(**task.store.load({part_outfile!r})))
task.store.save({infile!r}, pspace.build())
LoadBalancingWorker.create_statusfile({statusfile!r})
        '''.format(
            cont=cont, infile=self.infile, outfile=self.resultfile,
            part_outfile=self.partial_resultfile, statusfile=self.statusfile)
        file_dep = [os.path.join(os.path.dirname(self.task.path), f)
                    for f in self.task.file_dep]
        return Job(
            'pspace', self.submit, code, [self.task.path] + file_dep,
            [self.infile])

    # FIXME creates a bunch of identical python files.
    def create_process_job(self):
        jobs = []
        code = '''
from psyrun.processing import LoadBalancingWorker
LoadBalancingWorker({infile!r}, {outfile!r}, {statusfile!r}, task.store).start(
    task.execute)
        '''.format(
            infile=self.infile, outfile=self.partial_resultfile,
            statusfile=self.statusfile)
        for i in range(self.task.max_jobs):
            jobs.append(Job(
                str(i), self.submit, code, [self.infile],
                [self.partial_resultfile]))

        group = JobGroup('process', jobs)
        return group

    def create_finalize_job(self):
        code = '''
import os
os.rename({part!r}, {whole!r})
        '''.format(part=self.partial_resultfile, whole=self.resultfile)
        return Job(
            'finalize', self.submit, code, [self.partial_resultfile],
            [self.resultfile])

    def get_missing(self):
        pspace = self.task.pspace
        try:
            missing_items = missing(
                pspace, Param(**self.task.store.load(self.resultfile)))
        except IOError:
            try:
                missing_items = missing(
                    pspace,
                    Param(**self.task.store.load(self.partial_resultfile)))
            except IOError:
                pass
        return missing_items
