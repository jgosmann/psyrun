"""Base backend interface."""

import os
import sys


class JobSourceFile(object):
    """Describes a source code file for a job.

    Parameters
    ----------
    path : str
        Path to write the source file to.
    task : Task
        Task the source file corresponds to.
    job_code : str
        Job specific code to execute.

    Attributes
    ----------
    written : bool
        Whether the source file has been written.
    full_code : str
        The full job code including non job-specific parts that are used for
        every job.
    """

    def __init__(self, path, task, job_code):
        self.path = path
        self.task = task
        self.job_code = job_code
        self.written = False

    def write(self):
        """Write the job code to the file *self.path*."""
        with open(self.path, 'w') as f:
            f.write(self.full_code)
        self.written = True

    @property
    def full_code(self):
        return '''
try:
    import faulthandler
    faulthandler.enable()
except:
    pass

import time
print("")
print("----------------------------------------------------------------------")
print("Job started ({{}})".format(time.strftime('%a, %d %b %Y %H:%M:%S, %Z')))
print("----------------------------------------------------------------------")

import os
os.chdir({taskdir!r})

from psyrun.tasks import TaskDef
task = TaskDef({taskpath!r})
{code}
        '''.format(
            path=sys.path,
            taskdir=os.path.abspath(os.path.dirname(self.task.path)),
            taskpath=os.path.abspath(self.task.path), code=self.job_code)


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
    workdir : str
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
        code: str
            Code to execute in job.
        name: str
            Job name.
        depends_on: sequence
            Job IDs that have to finish before the submitted code can be
            executed.

        Returns
        -------
        dict
            Contains the id of the submitted job under the key ``'id'``.
        """
        codefile = os.path.join(self.workdir, name + '.py')
        output_filename = os.path.join(self.workdir, name + '.log')
        JobSourceFile(codefile, self.task, code).write()

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
        cont : bool, optional
            By default old results will be discarded, but when this option is
            set to True, old results will be kept and merged with the new
            results.
        """
        raise NotImplementedError()

    def get_missing(self):
        """Returns a `ParameterSpace` with missing parameter assignments.

        Missing paramaters assignments are parameter assignments requested by
        the task definition, but that have not been evaluated yet.
        """
        raise NotImplementedError
