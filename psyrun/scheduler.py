import os
import os.path
import subprocess


class Scheduler(object):
    def submit(
            self, args, depends_on=None, output_file=None,
            scheduler_args=None):
        """Returns job id."""
        raise NotImplementedError()

    def kill(self, jobid):
        raise NotImplementedError()

    def get_status(self, jobid):
        raise NotImplementedError()


class ImmediateRun(Scheduler):
    def submit(
            self, args, depends_on=None, output_file=None,
            scheduler_args=None):
        subprocess.call(args)
        return 0

    def kill(self, jobid):
        pass

    def get_status(self, jobid):
        pass


class Sqsub(Scheduler):
    class _Option(object):
        def __init__(self, name, conversion=str):
            self.name = name
            self.conversion = conversion

        def build(self, value):
            raise NotImplementedError()

    class _ShortOption(_Option):
        def build(self, value):
            return [self.name, self.conversion(value)]

    class _LongOption(_Option):
        def build(self, value):
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
            args.append(self.KNOWN_ARGS[k].build(v))
        return args

    def __init__(self, workdir=None):
        if workdir is None:
            workdir = '/work/{user}/psyrun'.format(user=os.environ['USER'])
        if not os.path.exists(workdir):
            os.makedirs(workdir)
        self.workdir = workdir
        self.idfile = os.path.join(workdir, 'idfile')

    def submit(
            self, args, depends_on=None, output_file=None,
            scheduler_args=None):
        """Returns job id."""
        scheduler_args = dict(scheduler_args)
        scheduler_args['idfile'] = self.idfile
        if depends_on is not None:
            scheduler_args['depends_on'] = depends_on
        if output_file is not None:
            scheduler_args['output_file'] = output_file
        subprocess.check_call(
            ['sqsub'] + self.build_args(**scheduler_args) + args)
        with open(self.idfile, 'r') as f:
            return int(f.read())

    def kill(self, jobid):
        subprocess.check_call(['sqkill', jobid])

    def get_status(self, jobid):
        p = subprocess.check_call(['sqjobs', jobid], stdout=subprocess.PIPE)
        line = p.stdout.readline()
        while line != '':
            cols = line.split()
            if cols[0] == jobid:
                return cols[3]
            line = p.stdout.readline()
        return None
