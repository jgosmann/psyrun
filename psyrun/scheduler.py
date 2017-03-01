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
        args : sequence
            The command and arguments to execute.
        output_filename : str
            File to write process output to.
        name : str, optional
            Name of job.
        depends_on : sequence of int, optional
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
        `JobStatus`
            Returns a tuple with *(id, status, name)* wherein status can be

            * ``'Q'`` for a queued job
            * ``'*Q'`` for a queued job waiting on another job to finish
            * ``'Z'`` for a sleeping job
            * ``'D'`` for a completed job

            If no status data is available for the job ID, None will be
            returned.
        """
        raise NotImplementedError()

    def get_jobs(self):
        """Get all queued, running, and recently finished jobs.

        Returns
        -------
        sequence
            Job IDs
        """
        raise NotImplementedError()


class ImmediateRun(Scheduler):
    """Runs jobs immediately on the local machine."""

    _cur_id = 0
    _failed_jobs = []

    def submit(
            self, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        """Submit a job.

        Parameters
        ----------
        args : sequence
            The command and arguments to execute.
        output_filename : str
            File to write process output to.
        name
            Unused.
        depends_on
            Unused
        scheduler_args
            Unused.

        Returns
        -------
        int
            Job ID
        """
        self._cur_id += 1

        if any(x in self._failed_jobs for x in depends_on):
            self._failed_jobs.append(self._cur_id)
            return self._cur_id

        with open(output_filename, 'a') as f:
            if subprocess.call(args, stdout=f, stderr=subprocess.STDOUT) != 0:
                self._failed_jobs.append(self._cur_id)
        return self._cur_id

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
        self._jobs = None

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

        # Checking whether jobs depending on are completed before submitting
        # the new job is a race condition, but I see no possibility to avoid
        # it.
        if depends_on is not None:
            statii = [self.get_status(x) for x in depends_on]
            depends_on = [x for x, s in zip(depends_on, statii)
                          if s is not None and s.status != 'D']

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
            Returns a tuple with *(id, status, name)* wherein status can be

            - ``'Q'`` for a queued job
            - ``'*Q'`` for a queued job waiting on another job to finish
            - ``'Z'`` for a sleeping job
            - ``'D'`` for a completed job

            If no status data is available for the job ID, None will be
            returned.
        """
        if self._jobs is None:
            self.refresh_job_info()
        elif jobid not in self._jobs:
            try:
                stdout = subprocess.check_output(
                    ['sqjobs', str(jobid)], stderr=subprocess.STDOUT,
                    universal_newlines=True)
                for line in stdout.split(os.linesep)[2:]:
                    cols = line.split(None, 5)
                    if len(cols) > 3 and int(cols[0]) == jobid:
                        if cols[2] == 'C':
                            cols[2] = 'D'
                        self._jobs[jobid] = JobStatus(jobid, cols[2], cols[-1])
            except subprocess.CalledProcessError as err:
                # 1 if none pending, running, or suspended
                if err.returncode != 1:
                    raise
        return self._jobs.get(jobid, None)

    def get_jobs(self):
        """Get all queued and running jobs.

        Returns
        -------
        list of int
            Job IDs
        """
        if self._jobs is None:
            self.refresh_job_info()
        return self._jobs.keys()

    def refresh_job_info(self):
        self._jobs = {}
        stdout = subprocess.check_output(['sqjobs'], universal_newlines=True)
        for line in stdout.split('\n')[2:]:
            cols = line.split(None, 5)
            if len(cols) > 2 and cols[2] in ['Q', '*Q', 'Z']:
                jobid = int(cols[0])
                self._jobs[jobid] = JobStatus(jobid, cols[2], cols[-1])
