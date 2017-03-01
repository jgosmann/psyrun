"""Handling and processing of job trees."""

import itertools
import os
import os.path
import warnings

from psyrun.exceptions import JobsRunningWarning
from psyrun.utils.doc import inherit_docs


class Job(object):
    """Describes a single processing job.

    Parameters
    ----------
    name : str
        Name of the job.
    submit_fn : function
        Function to use to submit the job for processing.
    code : str
        Python code to execute.
    dependencies : sequence
        Identifiers of other jobs that need to finish first before this job
        can be run.
    targets : sequence of str
        Files created by this job.

    Attributes
    ----------
    name : str
        Name of the job.
    submit_fn : function
        Function to use to submit the job for processing.
    code : str
        Python code to execute.
    dependencies : sequence
        Identifiers of other jobs that need to finish first before this job
        can be run.
    targets : sequence of str
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
    name : str
        Name of the job chain.
    jobs : sequence of Job
        Jobs to run in succession.

    Attributes
    ----------
    name : str
        Name of the job chain.
    jobs : sequence of Job
        Jobs to run in succession.
    dependencies : sequence
        Jobs that need to run first before the job chain can be run (equivalent
        to the dependencies of the first job in the chain).
    targets : sequence of str
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
    name : str
        Name of the job group.
    jobs : sequence of Job
        Jobs to run in the job group.

    Attributes
    ----------
    name : str
        Name of the job group.
    jobs : sequence of Job
        Jobs to run in the job group.
    dependencies : sequence
        Jobs that need to run first before the job group can be run (equivalent
        to the union of all the group's job's dependencies).
    targets : sequence of str
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
    names : dict
        Maps jobs to their names. (Can be obtained with `Fullname`.)
    uptodate : dict
        Maps jobs to their up-to-date status.
        (Can be obtained with `Uptodate`.)

    Attributes
    ----------
    names : dict
        Maps jobs to their names.
    uptodate : dict
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

    The constructor will call visit.

    Parameters
    ----------
    job : job tree
        Tree of jobs to clean.
    task : TaskDef
        Task that generated the job tree.
    names : dict
        Maps jobs to their names. (Can be obtained with Fullname.)
    uptodate : dict, optional
        Maps jobs to their up-to-date status.
        (Can be obtained with Uptodate.)
        If not provided, all jobs are treated as outdated.

    Attributes
    ----------
    task : TaskDef
        Task that generated the job tree.
    names : dict
        Maps jobs to their names.
    uptodate : dict
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
    names : dict
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

    The constructor will call visit.

    Parameters
    ----------
    jobtree : job tree
        Tree of jobs to determine the up-to-date status for.
    names : dict
        Maps jobs to their names. (Can be obtained with Fullname.)
    task : TaskDef
        Task that generated the job tree.

    Attributes
    ----------
    names : dict
        Maps jobs to their names.
    task : TaskDef
        Task that generated the job tree.
    status : dict
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
