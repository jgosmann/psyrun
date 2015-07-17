import os.path
import pickle
import subprocess

from psyrun.scheduler import JobStatus, Scheduler


class MockScheduler(Scheduler):
    def __init__(self, datafile):
        self.datafile = datafile

    @property
    def joblist(self):
        if os.path.exists(self.datafile):
            with open(self.datafile, 'rb') as f:
                return pickle.load(f)
        else:
            return tuple()

    @joblist.setter
    def joblist(self, value):
        with open(self.datafile, 'wb') as f:
            pickle.dump(tuple(value), f, pickle.HIGHEST_PROTOCOL)

    def submit(
            self, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        if depends_on is None:
            depends_on = []
        jobid = len(self.joblist)
        self.joblist = self.joblist + ({
            'id': jobid,
            'args': args,
            'output_filename': output_filename,
            'name': name or str(jobid),
            'depends_on': depends_on,
            'scheduler_args': scheduler_args,
            'status': '*Q' if len(depends_on) > 0 else 'Q',
        },)
        return jobid

    def kill(self, jobid):
        self.joblist = [job for job in self.joblist if job['id'] != jobid]

    def get_status(self, jobid):
        for job in self.joblist:
            if job['id'] == jobid:
                return JobStatus(job['id'], job['status'], job['name'])
        return None

    def consume(self):
        for job in self.joblist:
            with open(job['output_filename'], 'a') as f:
                subprocess.call(
                    job['args'], stdout=f, stderr=subprocess.STDOUT)
        self.joblist = []

    def get_jobs(self):
        return [job['id'] for job in self.joblist]
