"""Load balancing backend."""

import fcntl
import os

from psyrun.backend.base import Backend, JobSourceFile
from psyrun.jobs import Job, JobArray, JobChain
from psyrun.mapper import map_pspace
from psyrun.pspace import missing, Param
from psyrun.store import DefaultStore
from psyrun.utils.doc import inherit_docs


@inherit_docs
class LoadBalancingBackend(Backend):
    """Create subtasks for load balanced parameter evaluation.

    This will create *max_jobs* worker jobs that will fetch parameter
    assignments from a queue. Thus, all worker jobs should be busy all of the
    time.

    This backend is useful if individual parameters assignments can vary to a
    large degree in their processing time. It is recommended to use a `Store`
    that supports efficient fetching of a single row from a file.

    Parameters
    ----------
    task : `TaskDef`
        Task definition to create subtasks for.
    """

    @property
    def infile(self):
        """File from which input parameter assignments are fetched."""
        return os.path.join(self.workdir, 'in' + self.task.store.ext)

    @property
    def statusfile(self):
        """File that stores the processing status."""
        return os.path.join(self.workdir, 'status')

    @property
    def partial_resultfile(self):
        """File results are appended to while processing is in progress."""
        root, ext = os.path.splitext(self.resultfile)
        return root + '.part' + ext

    @property
    def resultfile(self):
        """Final result file."""
        if self.task.resultfile:
            return self.task.resultfile
        else:
            return os.path.join(self.workdir, 'result' + self.task.store.ext)

    def create_job(self, cont=False):
        pspace = self.create_pspace_job(cont=cont)
        process = self.create_process_job()
        finalize = self.create_finalize_job()
        return JobChain(self.task.name, [pspace, process, finalize])

    def create_pspace_job(self, cont):
        code = '''
import os.path
from psyrun.pspace import missing, Param
from psyrun.backend.load_balancing import LoadBalancingWorker
pspace = task.pspace
if {cont!r}:
    if os.path.exists({outfile!r}):
        os.rename({outfile!r}, {part_outfile!r})
    if os.path.exists({part_outfile!r}):
        pspace = missing(pspace, Param(**task.store.load({part_outfile!r})))
task.store.save({infile!r}, pspace.build())
LoadBalancingWorker.create_statusfile({statusfile!r})
        '''.format(
            cont=cont, infile=self.infile, outfile=self.resultfile,
            part_outfile=self.partial_resultfile, statusfile=self.statusfile)
        file_dep = [os.path.join(os.path.dirname(self.task.path), f)
                    for f in self.task.file_dep]
        return Job(
            'pspace', self.submit_code, {'code': code},
            [self.task.path] + file_dep, [self.infile])

    def create_process_job(self):
        source_file = JobSourceFile(
            os.path.join(self.workdir, self.task.name + ':process.py'),
            self.task,
            '''
from multiprocessing import Process
import sys
from psyrun.backend.load_balancing import LoadBalancingWorker
if __name__ == '__main__':
    workers = [
        LoadBalancingWorker(sys.argv[1], sys.argv[2], sys.argv[3], task.store)
        for _ in range({pool_size})]
    processes = [Process(target=w.start, args=(task.execute,))
                 for w in workers]
    for p in processes:
        p.start()
    for p in processes:
        p.join()
            '''.format(pool_size=self.task.pool_size))

        return JobArray(
            self.task.max_jobs, 'process', self.submit_array, self.submit_file,
            {'job_source_file': source_file, 'args': [
                self.infile, self.partial_resultfile, self.statusfile
            ]}, [self.infile], [self.partial_resultfile])

    def create_finalize_job(self):
        code = '''
import os
os.rename({part!r}, {whole!r})
        '''.format(part=self.partial_resultfile, whole=self.resultfile)
        return Job(
            'finalize', self.submit_code, {'code': code},
            [self.partial_resultfile],
            [self.resultfile])

    def get_missing(self):
        missing_items = self.task.pspace
        try:
            missing_items = missing(
                missing_items, Param(**self.task.store.load(self.resultfile)))
        except IOError:
            try:
                missing_items = missing(
                    missing_items,
                    Param(**self.task.store.load(self.partial_resultfile)))
            except IOError:
                pass
        return missing_items

    def get_queued(self):
        return None

    def get_failed(self):
        return None


class LoadBalancingWorker(object):
    """Maps a function to the parameter space supporting other
    *LoadBalancingWorkers* processing the same input file at the same time.

    Parameters
    ----------
    infile : str
        Filename of the file with the input parameters space.
    outfile : str
        Filename of the file to write the results to.
    statusfile : str
        Filename of the file to track the processing progress.
    store : `Store`, optional
        Input/output backend.
    """
    def __init__(self, infile, outfile, statusfile, store=DefaultStore()):
        self.infile = infile
        self.outfile = outfile
        self.statusfile = statusfile
        self.store = store

    @classmethod
    def create_statusfile(cls, statusfile):
        """Creates the status file required by all load balancing workers.

        Parameters
        ----------
        statusfile : str
            Filename of the status file.
        """
        with open(statusfile, 'w') as f:
            f.write('0')
            f.flush()

    def get_next_ix(self):
        """Get the index of the next parameter assignment to process."""
        with open(self.statusfile, 'r+') as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                ix = int(f.read())
                f.seek(0)
                f.truncate(0)
                f.write(str(ix + 1))
                f.flush()
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
        return ix

    def get_next_param_set(self):
        """Load the next parameter assignment to process."""
        return self.store.load(self.infile, row=self.get_next_ix())

    def save_data(self, data):
        """Appends data to the *outfile*.

        Uses a lock on the file to support concurrent access.
        """
        with open(self.statusfile + '.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                self.store.append(self.outfile, data)
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def start(self, fn):
        """Start processing a parameter space.

        A status file needs to be created before invoking this function by
        calling `create_statusfile`.

        Parameters
        ----------
        fn : function
            Function to evaluate on the parameter space.
        """
        while True:
            try:
                pspace = Param(**self.get_next_param_set())
            except IndexError:
                return
            data = map_pspace(fn, pspace)
            self.save_data(data)
