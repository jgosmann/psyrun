"""Job scheduler."""

from collections import namedtuple
import os
import os.path
import subprocess


JobStatus = namedtuple('JobStatus', ['id', 'status', 'name'])


class Scheduler(object):
    """Scheduler interface."""

    def submit(
            self, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        """Submit a job.

        Parameters
        ----------
        args : list
            The command and arguments to execute.
        output_filename : str
            File to write process output to.
        name : str, optional
            Name of job.
        depends_on : list of int, optional
            IDs of jobs that need to finish first before the submitted job can
            be started.
        scheduler_args : dict, optional
            Additional arguments for the scheduler.

        Returns
        -------
        Job ID
        """
        raise NotImplementedError()

    def kill(self, jobid):
        """Kill a job.

        Parameters
        ----------
        jobid
            Job to kill.
        """
        raise NotImplementedError()

    def get_status(self, jobid):
        """Get the status of a job.

        Parameters
        ----------
        jobid
            Job to request status of.

        Returns
        -------
        namedtuple
            Returns a tuple with `(id, status, name)` wherein status can be
            - `Q` for a queued job
            - `*Q` for a queued job waiting on another job to finish
            - `Z` for a sleeping job
            - `D` for a completed job
            If no status data is available for the job ID, ``None`` will be
            returned.
        """
        raise NotImplementedError()

    def get_jobs(self):
        """Get all queued, running, and recently finished jobs.

        Returns
        -------
        list
            Job IDs
        """
        raise NotImplementedError()


class ImmediateRun(Scheduler):
    """Runs jobs immediatly on the local machine."""

    def submit(
            self, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        """Submit a job.

        Parameters
        ----------
        args : list
            The command and arguments to execute.
        output_filename : str
            File to write process output to.
        name : str, optional
            Unused.
        depends_on : list of int, optional
            Unused
        scheduler_args : ``None``, optional
            Unused.

        Returns
        -------
        int
            0
        """
        with open(output_filename, 'a') as f:
            subprocess.call(args, stdout=f, stderr=subprocess.STDOUT)
        return 0

    def kill(self, jobid):
        """Has no effect."""
        pass

    def get_status(self, jobid):
        """Has no effect."""
        pass

    def get_jobs(self):
        """Returns an empty list."""
        return []


class Sqsub(Scheduler):
    """sqsub (sharcnet) scheduler."""

    class _Option(object):
        def __init__(self, name, conversion=str):
            self.name = name
            self.conversion = conversion

        def build(self, value):
            raise NotImplementedError()

    class _ShortOption(_Option):
        def build(self, value):
            if value is None:
                return []
            return [self.name, self.conversion(value)]

    class _LongOption(_Option):
        def build(self, value):
            if value is None:
                return []
            return [self.name + '=' + self.conversion(value)]

    KNOWN_ARGS = {
        'timelimit': _ShortOption('-r'),
        'output_file': _ShortOption('-o'),
        'n_cpus': _ShortOption('-n'),
        'n_nodes': _ShortOption('-N'),
        'memory': _LongOption('--mpp'),
        'depends_on': _ShortOption(
            '-w', lambda jobids: ','.join(str(x) for x in jobids)),
        'idfile': _LongOption('--idfile'),
        'name': _ShortOption('-j')
    }

    def build_args(self, **kwargs):
        args = []
        for k, v in kwargs.items():
            args.extend(self.KNOWN_ARGS[k].build(v))
        return args

    def __init__(self, workdir=None):
        if workdir is None:
            workdir = '/work/{user}/psyrun'.format(user=os.environ['USER'])
        if not os.path.exists(workdir):
            os.makedirs(workdir)
        self.workdir = workdir
        self.idfile = os.path.join(workdir, 'idfile')

    def submit(
            self, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        """Submit a job.

        Parameters
        ----------
        args : list
            The command and arguments to execute.
        output_filename : str
            File to write process output to.
        name : str, optional
            Name of job.
        depends_on : list of int, optional
            IDs of jobs that need to finish first before the submitted job can
            be started.
        scheduler_args : dict, optional
            Additional arguments for the scheduler.

        Returns
        -------
        int
            Job ID
        """
        if scheduler_args is None:
            scheduler_args = dict()
        else:
            scheduler_args = dict(scheduler_args)
        scheduler_args.update({
            'idfile': self.idfile,
            'output_file': output_filename,
            'depends_on': depends_on,
            'name': name,
        })
        subprocess.check_call(
            ['sqsub'] + self.build_args(**scheduler_args) + args)
        with open(self.idfile, 'r') as f:
            return int(f.read())

    def kill(self, jobid):
        """Kill a job.

        Parameters
        ----------
        jobid : int
            Job to kill.
        """
        subprocess.check_call(['sqkill', str(jobid)])

    def get_status(self, jobid=None):
        """Get the status of a job.

        Parameters
        ----------
        jobid : int
            Job to request status of.

        Returns
        -------
        namedtuple or None
            Returns a tuple with `(id, status, name)` wherein status can be
            - `Q` for a queued job
            - `*Q` for a queued job waiting on another job to finish
            - `Z` for a sleeping job
            - `D` for a completed job
            If no status data is available for the job ID, ``None`` will be
            returned.
        """
        stdout = subprocess.check_output(['sqjobs', str(jobid)])
        for line in stdout.split(os.linesep)[2:]:
            cols = line.split(None, 6)
            if int(cols[0]) == jobid:
                if cols[2] == 'C':
                    cols[2] = 'D'
                return JobStatus(cols[0], cols[2], cols[6])
        return None

    def get_jobs(self):
        """Get all queued and running jobs.

        Returns
        -------
        list of int
            Job IDs
        """
        jobs = []
        stdout = subprocess.check_output(['sqjobs'])
        for line in stdout.split(os.linesep)[2:]:
            cols = line.split(None, 3)
            if len(cols) > 2 and cols[2] in ['Q', '*Q', 'Z']:
                jobs.append(int(cols[0]))
        return jobs
