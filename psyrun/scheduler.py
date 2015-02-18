import os
import os.path
import subprocess


class Scheduler(object):
    def submit(self, args, depends_on=None, scheduler_args=None):
        """Returns job id."""
        raise NotImplementedError()

    def kill(self, jobid):
        raise NotImplementedError()

    def get_status(self, jobid):
        raise NotImplementedError()


class ImmediateRun(Scheduler):
    def submit(self, args, depends_on=None, scheduler_args=None):
        subprocess.call(args)
        return 0

    def kill(self, jobid):
        pass

    def get_status(self, jobid):
        pass


class Sqsub(Scheduler):
    class Args(object):
        def __init__(self, copy_from=None):
            self.arg_list = []
            if copy_from is not None:
                self.arg_list.extend(copy_from.arg_list)

        def timelimit(self, limit):
            self.arg_list.extend(['-r', limit])

        def output_file(self, filename):
            self.arg_list.extend(['-o', filename])

        def n_cpus(self, n_cpus):
            self.arg_list.extend(['-n', n_cpus])

        def n_nodes(self, n_nodes):
            self.arg_list.extend(['-N', n_nodes])

        def memory(self, limit):
            self.arg_list.extend(['--mpp=' + limit])

        def depends_on(self, jobids):
            self.arg_list.extend(['-w', ','.join(jobids)])

        def idfile(self, idfile):
            self.arg_list.extend(['--idfile=' + idfile])

        def name(self, name):
            self.arg_list.extend(['-j', name])


    def __init__(self, workdir=None):
        if workdir is None:
            workdir = '/work/{user}/psyrun'.format(user=os.environ['USER'])
        if not os.path.exists(workdir):
            os.makedirs(workdir)
        self.workdir = workdir
        self.idfile = os.path.join(workdir, 'idfile')

    def submit(self, args, depends_on=None, scheduler_args=None):
        """Returns job id."""
        scheduler_args = self.Args(scheduler_args).idfile(self.idfile)
        if depends_on is not None:
            scheduler_args.depends_on(depends_on)
        subprocess.check_call(['sqsub'] + scheduler_args.arg_list + args)
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
