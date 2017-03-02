"""Testing utilities."""

import os.path
import pickle
import subprocess

from psyrun.scheduler import JobStatus, Scheduler
from psyrun.utils.doc import inherit_docs


@inherit_docs
class MockScheduler(Scheduler):
    """Mock scheduler implementation.

    Parameters
    ----------
    datafile : str
        File to store supporting data.
    """

    def __init__(self, datafile):
        self.datafile = datafile

    @property
    def next_id(self):
        """ID for next submitted job."""
        if os.path.exists(self.datafile):
            with open(self.datafile, 'rb') as f:
                return pickle.load(f)['next_id']
        else:
            return 0

    @next_id.setter
    def next_id(self, value):
        self._serialize(next_id=value)

    @property
    def joblist(self):
        """Tuple of current jobs."""
        if os.path.exists(self.datafile):
            with open(self.datafile, 'rb') as f:
                return tuple(pickle.load(f)['joblist'])
        else:
            return tuple()

    @joblist.setter
    def joblist(self, value):
        self._serialize(joblist=value)

    def _serialize(self, next_id=None, joblist=None):
        if next_id is None:
            next_id = self.next_id
        if joblist is None:
            joblist = self.joblist

        with open(self.datafile, 'wb') as f:
            pickle.dump(
                {'next_id': next_id, 'joblist': joblist},
                f, pickle.HIGHEST_PROTOCOL)

    def submit(
            self, args, output_filename, name=None, depends_on=None,
            scheduler_args=None):
        if depends_on is None:
            depends_on = []
        jobid = self.next_id
        self.next_id += 1
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

    def mark_running(self):
        """Mark all jobs as running."""
        updated = []
        for job in self.joblist:
            job.update({'status': 'R'})
            updated.append(job)
        self.joblist = tuple(updated)

    def kill(self, jobid):
        self.joblist = [job for job in self.joblist if job['id'] != jobid]

    def get_status(self, jobid):
        for job in self.joblist:
            if job['id'] == jobid:
                return JobStatus(job['id'], job['status'], job['name'])
        return None

    def consume(self):
        """Process all queued jobs."""
        for job in self.joblist:
            with open(job['output_filename'], 'a') as f:
                subprocess.check_call(
                    job['args'], stdout=f, stderr=subprocess.STDOUT)
        self.joblist = []

    def consume_job(self, job):
        """Process a  single job."""
        with open(job['output_filename'], 'a') as f:
            subprocess.check_call(
                job['args'], stdout=f, stderr=subprocess.STDOUT)
        self.joblist = [j for j in self.joblist if j != job]

    def get_jobs(self):
        return [job['id'] for job in self.joblist]
