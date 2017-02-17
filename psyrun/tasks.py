from __future__ import print_function

import itertools
import os
import os.path
import re
import sys
import traceback
import warnings

from psyrun.store import NpzStore
from psyrun.pspace import Param
from psyrun.processing import Splitter
from psyrun.scheduler import ImmediateRun


class JobsRunningWarning(UserWarning):
    def __init__(self, name):
        self.task = name
        super(UserWarning, self).__init__(
            "Task '{}' has unfinished jobs queued. Not starting new jobs "
            "until these are finished or have been killed.".format(name))


class TaskWorkdirDirtyWarning(UserWarning):
    def __init__(self, name):
        # TODO explain how to solve this
        self.task = name
        super(UserWarning, self).__init__(
            "Work directory of task '{}' is dirty.".format(name))


warnings.simplefilter('always', category=UserWarning)


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
    resultfile : str or None
        Path to save the results of the finished task at. If ``None``, this
        defaults to ``'result.<ext>'`` in the `workdir`.
    scheduler : :class:`.Scheduler`
        Scheduler to use to submit individual jobs. Defaults to
        :class:`.ImmediateRun`.
    scheduler_args : dict
        Additional scheduler arguments.
    python : str
        Path to Python interpreter to use.
    max_jobs : int
        Maximum number of splits of the parameter space. This limits the number
        of jobs started.
    min_items : int
        Minimum number of parameter sets to evaluate per job.
    backend : TODO
        TODO
    file_dep : list of str
        Additional files the task depends on.
    store : :class:`.store.AbstractStore`
        Input/output backend. Defaults to :class:`NpzStore`.
    """

    __slots__ = [
        'workdir', 'resultfile', 'scheduler', 'pspace', 'backend',
        'scheduler_args', 'python', 'max_jobs', 'min_items', 'file_dep',
        'store', 'overwrite_dirty']

    def __init__(self):
        self.workdir = os.path.abspath('psy-work')
        self.resultfile = None
        self.scheduler = ImmediateRun()
        self.scheduler_args = dict()
        self.pspace = Param()
        self.python = sys.executable
        self.max_jobs = 64
        self.min_items = 4
        self.backend = DistributeBackend
        self.file_dep = []
        self.store = NpzStore()
        self.overwrite_dirty = True

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


class PackageLoader(object):
    """Loads tasks from Python files.

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

    def load_task_defs(self):
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

    def load_tasks(self, cmd, opt_values, pos_args):
        task_list = []
        for task_def in self.load_task_defs():
            task_list.extend(self.create_task(task_def))
        return task_list, {}

    def create_task(self, task):
        creator = task.backend(task)
        return creator.create_job()


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


class Submit(JobTreeVisitor):
    def __init__(self, job, names, uptodate):
        super(Submit, self).__init__()
        self.names = names
        self.uptodate = uptodate
        self.depends_on = []
        self.visit(job)

    def visit_job(self, job):
        if self.uptodate.status[job]:
            print('-', self.names[job])
            return []
        else:
            print('.', self.names[job])
            return [job.submit_fn(
                job.code, self.names[job], depends_on=self.depends_on)]

    def visit_group(self, group):
        return sum((self.visit(job) for job in group.jobs), [])

    def visit_chain(self, chain):
        old_depends_on = self.depends_on
        job_ids = []
        for job in chain.jobs:
            ids = self.visit(job)
            job_ids.extend(ids)
            self.depends_on = old_depends_on + ids
        self.depends_on = old_depends_on
        return job_ids


class Clean(JobTreeVisitor):
    def __init__(self, job, task, names):
        super(Submit, self).__init__()
        self.task = task
        self.names = names
        self.visit(job)

    def visit_job(self, job):
        workdir = os.path.join(self.task.workdir, self.task.name)
        for item in os.listdir(workdir):
            if item.startswith(self.names[job]):
                os.remove(os.path.join(workdir, item))
        for t in job.targets:
            if os.path.exists(t):
                os.remove(t)


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
    def __init__(self, jobtree, names, task):
        super(Uptodate, self).__init__()
        self.names = names
        self.task = task
        self.status = {}
        self.clamp = None

        self.any_queued = False
        self.outdated = False

        self.visit(jobtree)
        self.post_visit()

    def post_visit(self):
        skip = False
        if self.any_queued and self.outdated:
            skip = True
            warnings.warn(JobsRunningWarning(self.task.name))
        elif self.outdated and not self._is_workdir_clean():
            if not self.task.overwrite_dirty:
                skip = True
                warnings.warn(TaskWorkdirDirtyWarning(self.task.name))

        if skip:
            for k in self.status.keys():
                self.status[k] = True

    def _is_workdir_clean(self):
        workdir = os.path.join(self.task.workdir, self.task.name)
        if not os.path.exists(workdir):
            return True
        else:
            for dirpath, dirnames, filenames in os.walk(workdir):
                if len(filenames) > 0:
                    return False
            return True

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
            self.task.scheduler.get_status(j).name
            for j in self.task.scheduler.get_jobs()]
        is_queued = self.names[job] in job_names
        self.any_queued |= is_queued
        return is_queued

    def files_uptodate(self, tref, targets):
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


class AbstractBackend(object):
    def __init__(self, task):
        super(AbstractBackend, self).__init__()
        self.task = task
        self.workdir = os.path.join(task.workdir, task.name)
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

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
        codefile = os.path.join(self.workdir, name + '.py')
        output_filename = os.path.join(self.workdir, name + '.log')
        with open(codefile, 'w') as f:
            f.write(code)

        for job in self.task.scheduler.get_jobs():
            status = self.task.scheduler.get_status(job)
            if status is not None and name == status.name:
                self.task.scheduler.kill(job)

        return {'id': self.task.scheduler.submit(
            [self.task.python, codefile], output_filename, name, depends_on,
            self.task.scheduler_args)}

    # def create_subtasks(self):
        # job = self.create_job()
        # names = Fullname(job).names
        # return ToDoitTask(self.task, names, Uptodate(
            # job, names, self.task).status).visit(job)

    def create_job(self):
        raise NotImplementedError()


class DistributeBackend(AbstractBackend):
    """Create subtasks for to distribute parameter evaluations.

    Parameters
    ----------
    task : :class:`.TaskDef`
        Task definition to create subtasks for.
    """

    def __init__(self, task):
        super(DistributeBackend, self).__init__(task)
        self.splitter = Splitter(
            self.workdir, task.pspace, task.max_jobs, task.min_items,
            store=task.store)

    @property
    def resultfile(self):
        if self.task.resultfile:
            return self.task.resultfile
        else:
            return os.path.join(
                self.splitter.workdir, 'result' + self.splitter.store.ext)

    def create_job(self):
        split = self.create_split_job()
        process = self.create_process_job()
        merge = self.create_merge_job()
        return JobChain(self.task.name, [split, process, merge])

    def create_split_job(self):
        code = '''
from psyrun.processing import Splitter
Splitter(
    {workdir!r}, task.pspace, {max_jobs!r}, {min_items!r},
    store=task.store).split()
        '''.format(
            workdir=self.splitter.workdir, max_jobs=self.task.max_jobs,
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
Worker(store=task.store).start(execute, {infile!r}, {outfile!r})
            '''.format(infile=infile, outfile=outfile)
            jobs.append(Job(str(i), self._submit, code, [infile], [outfile]))

        group = JobGroup('process', jobs)
        return group

    def create_merge_job(self):
        code = '''
from psyrun.processing import Splitter
Splitter.merge({outdir!r}, {filename!r}, append=False, store=task.store)
        '''.format(outdir=self.splitter.outdir, filename=self.resultfile)
        return Job(
            'merge', self._submit, code,
            [f for _, f in self.splitter.iter_in_out_files()],
            [self.resultfile])


class LoadBalancingBackend(AbstractBackend):
    @property
    def infile(self):
        return os.path.join(self.workdir, 'in' + self.task.store.ext)

    @property
    def statusfile(self):
        return os.path.join(self.workdir, 'status')

    @property
    def partial_resultfile(self):
        root, ext = os.path.splitext(self.resultfile)
        return root + '.part' + ext

    @property
    def resultfile(self):
        if self.task.resultfile:
            return self.task.resultfile
        else:
            return os.path.join(self.workdir, 'result' + self.task.store.ext)

    def create_job(self):
        pspace = self.create_pspace_job()
        process = self.create_process_job()
        finalize = self.create_finalize_job()
        return JobChain(self.task.name, [pspace, process, finalize])

    def create_pspace_job(self):
        code = '''
import os.path
from psyrun.pspace import missing, Param
from psyrun.processing import LoadBalancingWorker
if os.path.exists({outfile!r}):
    pspace = missing(task.pspace, Param.from_dict(task.store.load({outfile!r})))
else:
    pspace = task.pspace
task.store.save({infile!r}, pspace.build())
LoadBalancingWorker.create_statusfile({statusfile!r})
        '''.format(
            infile=self.infile, outfile=self.resultfile,
            statusfile=self.statusfile)
        file_dep = [os.path.join(os.path.dirname(self.task.path), f)
                    for f in self.task.file_dep]
        return Job(
            'pspace', self._submit, code, [self.task.path] + file_dep,
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
                str(i), self._submit, code, [self.infile],
                [self.partial_resultfile]))

        group = JobGroup('process', jobs)
        return group

    def create_finalize_job(self):
        code = '''
import os
os.rename({part!r}, {whole!r})
        '''.format(part=self.partial_resultfile, whole=self.resultfile)
        return Job(
            'finalize', self._submit, code, [self.partial_resultfile],
            [self.resultfile])
