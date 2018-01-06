"""Job scheduler."""

from collections import namedtuple
import getpass
import os
import os.path
import re
import subprocess
import sys


JobStatus = namedtuple('JobStatus', ['id', 'status', 'name'])


class Scheduler(object):
    """Scheduler interface."""

    USER_DEFAULT_ARGS = {}

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

    def submit_array(
            self, n, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        """Submit a job array.

        If the scheduler does not support job arrays, this method should raise
        *NotImplementedError*.

        Parameters
        ----------
        n : int
            Number of tasks to submit.
        args : sequence
            The command and arguments to execute. The string ``'%a'`` will be
            replaced with the task number in each argument.
        output_filename : str
            File to write process output to. The string ``'%a'`` will be
            replaced by the task number.
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
        if depends_on is None:
            depends_on = []

        self._cur_id += 1

        if any(x in self._failed_jobs for x in depends_on):
            self._failed_jobs.append(self._cur_id)
            return self._cur_id

        with open(output_filename, 'a') as f:
            if subprocess.call(args, stdout=f, stderr=subprocess.STDOUT) != 0:
                self._failed_jobs.append(self._cur_id)
        return self._cur_id

    def submit_array(
            self, n, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        raise NotImplementedError("Job arrays not supported.")

    def kill(self, jobid):
        """Has no effect."""
        pass

    def get_status(self, jobid):
        """Has no effect."""
        pass

    def get_jobs(self):
        """Returns an empty list."""
        return []


class ExternalScheduler(Scheduler):
    class Option(object):
        def __init__(self, name, conversion=str):
            self.name = name
            self.conversion = conversion

        def build(self, value):
            raise NotImplementedError()

        def __repr__(self):
            return "<{clsname} '{name}'>".format(
                clsname=self.__class__.__name__, name=self.name)

    class ShortOption(Option):
        def build(self, value):
            if value is None:
                return []
            return [self.name, self.conversion(value)]

    class LongOption(Option):
        def build(self, value):
            if value is None:
                return []
            return [self.name + '=' + self.conversion(value)]

    KNOWN_ARGS = {}

    def build_args(self, **kwargs):
        args = []
        for k, v in kwargs.items():
            args.extend(self.KNOWN_ARGS[k].build(v))
        return args


class Sqsub(ExternalScheduler):
    """sqsub (sharcnet) scheduler."""

    USER_DEFAULT_ARGS = {
        'timelimit': '1h',
        'n_cpus': 1,
        'n_nodes': 1,
        'memory': '1G',
    }

    KNOWN_ARGS = {
        'timelimit': ExternalScheduler.ShortOption('-r'),
        'output_file': ExternalScheduler.ShortOption('-o'),
        'n_cpus': ExternalScheduler.ShortOption('-n'),
        'n_nodes': ExternalScheduler.ShortOption('-N'),
        'memory': ExternalScheduler.LongOption('--mpp'),
        'depends_on': ExternalScheduler.ShortOption(
            '-w', lambda jobids: ','.join(str(x) for x in jobids)),
        'idfile': ExternalScheduler.LongOption('--idfile'),
        'name': ExternalScheduler.ShortOption('-j')
    }

    def __init__(self, workdir=None):
        super(Sqsub, self).__init__()

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
            scheduler_args = {k: v(name) if callable(v) else v
                              for k, v in scheduler_args.items()}

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

    def submit_array(
            self, n, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        raise NotImplementedError("Job arrays not supported.")

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

            - ``'R'`` for a running job
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
                for line in stdout.split('\n')[2:]:
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
            if len(cols) > 2 and cols[2] in ['R', 'Q', '*Q', 'Z']:
                jobid = int(cols[0])
                self._jobs[jobid] = JobStatus(jobid, cols[2], cols[-1])


class Slurm(ExternalScheduler):
    """Slurm (sbatch) scheduler."""

    USER_DEFAULT_ARGS = {
        'timelimit': '1h',
        'memory': '1G',
    }

    KNOWN_ARGS = {
        'timelimit': ExternalScheduler.ShortOption('-t'),
        'output_file': ExternalScheduler.ShortOption('-o'),
        'cores-per-socket': ExternalScheduler.LongOption('--cores-per-socket'),
        'sockets-per-node': ExternalScheduler.LongOption('--sockets-per-node'),
        'n_cpus': ExternalScheduler.ShortOption('-c'),
        'n_nodes': ExternalScheduler.ShortOption('-N'),
        'memory': ExternalScheduler.LongOption('--mem'),
        'memory_per_cpu': ExternalScheduler.LongOption('--mem-per-cpu'),
        'depends_on': ExternalScheduler.ShortOption(
            '-d', lambda jobids: 'afterok:' + ':'.join(
                str(x) for x in jobids)),
        'name': ExternalScheduler.ShortOption('-J'),
        'array': ExternalScheduler.LongOption('--array'),
    }

    STATUS_MAP = {
        'PD': 'Q',
        'R': 'R',
        'CA': 'D',
        'CF': 'R',
        'CG': 'R',
        'CD': 'D',
        'F': 'D',
        'TO': 'D',
        'NF': 'D',
        'RV': 'D',
        'SE': 'D',
    }

    def __init__(self, workdir=None):
        super(Slurm, self).__init__()
        if workdir is None:
            workdir = '/project/{user}/psyrun'.format(user=os.environ['USER'])
        if not os.path.exists(workdir):
            os.makedirs(workdir)
        self.workdir = workdir
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
        str
            Job ID
        """
        out = subprocess.check_output(self._prepare_submisson_cmd(
            args, output_filename, name, scheduler_args, depends_on))
        out = out.decode('utf-8')
        return out.split(None)[-1]

    def submit_array(
            self, n, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        if scheduler_args is None:
            scheduler_args = dict()
        else:
            scheduler_args = dict(scheduler_args)

        scheduler_args['array'] = '0-' + str(n - 1)

        code = '''#!{python}
import os
import subprocess
import sys

task_id = os.environ['SLURM_ARRAY_TASK_ID']
sys.exit(subprocess.call([a.replace('%a', str(task_id)) for a in {args!r}]))
'''.format(python=sys.executable, args=args)

        cmd = self._prepare_submisson_cmd(
            [], output_filename, name, scheduler_args, depends_on)
        p = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        out, _ = p.communicate(code.encode('utf-8'))
        out = out.decode('utf-8')
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, cmd, out)
        return out.split(None)[-1]

    def _prepare_submisson_cmd(
            self, args, output_filename, name=None, scheduler_args=None,
            depends_on=None):
        if scheduler_args is None:
            scheduler_args = dict()
        else:
            scheduler_args = {k: v(name) if callable(v) else v
                              for k, v in scheduler_args.items()}

        # Checking whether jobs depending on are completed before submitting
        # the new job is a race condition, but I see no possibility to avoid
        # it.
        if depends_on is not None:
            statii = [self.get_status(x) for x in depends_on]
            depends_on = [x for x, s in zip(depends_on, statii)
                          if s is not None and s.status != 'D']

        scheduler_args.update({
            'output_file': output_filename,
            'name': name,
        })
        if depends_on is not None and len(depends_on) > 0:
            scheduler_args['depends_on'] = depends_on
        return ['sbatch'] + self.build_args(**scheduler_args) + args

    def kill(self, jobid):
        """Kill a job.

        Parameters
        ----------
        jobid : str
            Job to kill.
        """
        subprocess.check_call(['scancel', str(jobid)])

    def get_status(self, jobid=None):
        """Get the status of a job.

        Parameters
        ----------
        jobid : str
            Job to request status of.

        Returns
        -------
        namedtuple or None
            Returns a tuple with *(id, status, name)* wherein status can be

            - ``'R'`` for a running job
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
            stdout = subprocess.check_output(
                ['squeue', '-u', getpass.getuser(), '-h', '-o', '%A %t %j %r',
                 '-j', str(jobid)],
                stderr=subprocess.STDOUT, universal_newlines=True)
            self._update_jobs(stdout)
        return self._jobs.get(jobid, None)

    def get_jobs(self):
        """Get all queued and running jobs.

        Returns
        -------
        list of str
            Job IDs
        """
        if self._jobs is None:
            self.refresh_job_info()
        return self._jobs.keys()

    def refresh_job_info(self):
        self._jobs = {}
        stdout = subprocess.check_output(
            ['squeue', '-u', getpass.getuser(), '-h', '-o', '%i %t %j %r'],
            universal_newlines=True)
        self._update_jobs(stdout)

    def _update_jobs(self, squeue_out):
        for line in squeue_out.split('\n'):
            cols = line.split(None, 4)
            if len(cols) > 3 and cols[1] in ['PD', 'R', 'CF', 'CG']:
                jobid = cols[0]
                status = self.STATUS_MAP[cols[1]]
                if status == 'Q' and cols[3] == 'Dependency':
                    status = '*Q'
                m = re.match(r'^(\d+)_\[(\d+)-(\d+)\]$', jobid)
                if m:
                    for i in range(int(m.group(2)), int(m.group(3)) + 1):
                        sub_id = m.group(1) + '_' + str(i)
                        self._jobs[sub_id] = JobStatus(
                            sub_id, status, cols[2] + ':' + str(i))
                else:
                    self._jobs[jobid] = JobStatus(jobid, status, cols[2])
