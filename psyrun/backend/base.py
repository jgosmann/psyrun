"""Base backend interface."""

import os
import sys


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
