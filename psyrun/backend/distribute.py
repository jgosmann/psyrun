"""Backend for distributed parameter evaluation."""

import os

from psyrun.backend.base import Backend
from psyrun.jobs import Job, JobChain, JobGroup
from psyrun.pspace import dict_concat, missing, Param
from psyrun.mapper import map_pspace_hdd_backed
from psyrun.store import DefaultStore
from psyrun.utils.doc import inherit_docs


@inherit_docs
class DistributeBackend(Backend):
    """Create subtasks for distributed parameter evaluation.

    This will create one tasks that splits the parameter space in a number of
    equal batches (at most *max_jobs*, but with at least *min_items* for each
    batch). After processing all batches the results will be merged into a
    single file.

    This is similar to map-reduce processing.

    Parameters
    ----------
    task : `TaskDef`
        Task definition to create subtasks for.
    """

    @property
    def resultfile(self):
        """File in which the results will be stored."""
        if self.task.resultfile:
            return self.task.resultfile
        else:
            return os.path.join(
                self.workdir, 'result' + self.task.store.ext)

    @property
    def pspace_file(self):
        """File that will store the input parameters space."""
        return os.path.join(self.workdir, 'pspace' + self.task.store.ext)

    def _try_mv_to_out(self, filename):
        try:
            os.rename(
                os.path.join(self.workdir, filename),
                os.path.join(self.workdir, 'out', filename))
        except OSError:
            pass

    def create_job(self, cont=False):
        if cont:
            outdir = os.path.join(self.workdir, 'out')
            self._try_mv_to_out('result' + self.task.store.ext)
            Splitter.merge(
                outdir, os.path.join(outdir, 'pre' + self.task.store.ext))
            for filename in os.listdir(outdir):
                if not filename.startswith('pre'):
                    os.remove(os.path.join(outdir, filename))
            pspace = self.get_missing()
        else:
            pspace = self.task.pspace

        self.task.store.save(self.pspace_file, pspace.build())
        splitter = Splitter(
            self.workdir, pspace, self.task.max_jobs, self.task.min_items,
            store=self.task.store)

        split = self.create_split_job(splitter)
        process = self.create_process_job(splitter)
        merge = self.create_merge_job(splitter)
        return JobChain(self.task.name, [split, process, merge])

    def create_split_job(self, splitter):
        code = '''
from psyrun.backend.distribute import Splitter
from psyrun.pspace import Param
pspace = Param(**task.store.load({pspace!r}))
Splitter(
    {workdir!r}, pspace, {max_jobs!r}, {min_items!r},
    store=task.store).split()
        '''.format(
            pspace=self.pspace_file,
            workdir=splitter.workdir, max_jobs=self.task.max_jobs,
            min_items=self.task.min_items)
        file_dep = [os.path.join(os.path.dirname(self.task.path), f)
                    for f in self.task.file_dep]
        return Job(
            'split', self.submit, code, [self.task.path] + file_dep,
            [f for f, _ in splitter.iter_in_out_files()])

    def create_process_job(self, splitter):
        jobs = []
        for i, (infile, outfile) in enumerate(
                splitter.iter_in_out_files()):
            code = '''
from psyrun.backend.distribute import Worker
execute = task.execute
Worker(store=task.store).start(execute, {infile!r}, {outfile!r})
            '''.format(infile=infile, outfile=outfile)
            jobs.append(Job(str(i), self.submit, code, [infile], [outfile]))

        group = JobGroup('process', jobs)
        return group

    def create_merge_job(self, splitter):
        code = '''
from psyrun.backend.distribute import Splitter
Splitter.merge({outdir!r}, {filename!r}, append=False, store=task.store)
        '''.format(outdir=splitter.outdir, filename=self.resultfile)
        return Job(
            'merge', self.submit, code,
            [f for _, f in splitter.iter_in_out_files()], [self.resultfile])

    def get_missing(self):
        pspace = self.task.pspace
        try:
            missing_items = missing(
                pspace, Param(**self.task.store.load(self.resultfile)))
        except IOError:
            missing_items = pspace
            try:
                for filename in os.listdir(os.path.join(self.workdir, 'out')):
                    outfile = os.path.join(self.workdir, 'out', filename)
                    try:
                        x = Param(**self.task.store.load(outfile))
                        missing_items = missing(
                            missing_items,
                            Param(**self.task.store.load(outfile)))
                    except IOError:
                        pass
            except IOError:
                pass
        return missing_items


class Splitter(object):
    """Split a parameter space into multiple input files and merge results
    after processing.

    Parameters
    ----------
    workdir : str
        Working directory to create input files in and read output files from.
    pspace : `ParameterSpace`
        Parameter space to split up.
    max_splits : int, optional
        Maximum number of splits to perform.
    min_items : int, optional
        Minimum number of parameter sets in each split.
    store : `Store`, optional
        Input/output backend.

    Attributes
    ----------
    indir : str
        Directory to store input files.
    max_splits : int
        Maximum number of splits to perform.
    min_items : int
        Minimum number of parameter sets in each split.
    outdir : str
        Directory to store output files.
    pspace : `ParameterSpace`
        Parameter space to split up.
    store : `Store`
        Input/output backend.
    workdir : str
        Working directory to create input files in and read output files from.
    """
    def __init__(
            self, workdir, pspace, max_splits=64, min_items=4,
            store=DefaultStore()):
        self.workdir = workdir
        self.indir = self._get_indir(workdir)
        self.outdir = self._get_outdir(workdir)

        if not os.path.exists(self.indir):
            os.makedirs(self.indir)
        if not os.path.exists(self.outdir):
            os.makedirs(self.outdir)

        self.pspace = pspace
        self.max_splits = max_splits
        self.min_items = min_items

        self.store = store

    @property
    def n_splits(self):
        """Number of total splits that will be generated."""
        n_splits = (len(self.pspace) - 1) // self.min_items + 1
        if self.max_splits is not None:
            n_splits = min(self.max_splits, n_splits)
        return n_splits

    def split(self):
        """Perform splitting of parameters space and save input files for
        processing."""
        items_remaining = len(self.pspace)
        param_iter = self.pspace.iterate()
        for i, filename in enumerate(self._iter_filenames()):
            split_size = self.min_items
            if self.max_splits is not None:
                split_size = max(
                    split_size, items_remaining // (self.max_splits - i))
            items_remaining -= split_size
            block = dict_concat(
                [row for row in self._iter_n(param_iter, split_size)])
            self.store.save(os.path.join(self.indir, filename), block)

    @classmethod
    def merge(cls, outdir, merged_filename, append=True, store=DefaultStore()):
        """Merge processed files together.

        Parameters
        ----------
        outdir : str
            Directory with the output files.
        merged_filename : str
            Filename of file to save with the merged results.
        append : bool, optional
            If True the merged data will be appended, otherwise the file
            will be overwritten with the merged data.
        store : `Store`, optional
            Input/output backend.
        """
        if not append:
            store.save(merged_filename, {})
        for filename in os.listdir(outdir):
            if os.path.splitext(filename)[1] != store.ext:
                continue
            infile = os.path.join(outdir, filename)
            store.append(merged_filename, store.load(infile))

    def iter_in_out_files(self):
        """Return generator returning tuples of corresponding input and output
        filenames."""
        return ((os.path.join(self.indir, f), os.path.join(self.outdir, f))
                for f in self._iter_filenames())

    def _iter_filenames(self):
        return (str(i) + self.store.ext for i in range(self.n_splits))

    @staticmethod
    def _iter_n(it, n):
        for _ in range(n):
            yield next(it)

    @classmethod
    def _get_indir(cls, workdir):
        return os.path.join(workdir, 'in')

    @classmethod
    def _get_outdir(cls, workdir):
        return os.path.join(workdir, 'out')


class Worker(object):
    """Maps a function to the parameter space loaded from a file and writes the
    result to an output file.

    Parameters
    ----------
    store : `Store`, optional
        Input/output backend.

    Attributes
    ----------
    store : `Store`
        Input/output backend.
    """

    def __init__(self, store=DefaultStore()):
        self.store = store

    def start(self, fn, infile, outfile):
        """Start processing a parameter space.

        Parameters
        ----------
        fn : function
            Function to evaluate on the parameter space.
        infile : str
            Parameter space input filename.
        outfile : str
            Output filename for the results.
        """
        pspace = Param(**self.store.load(infile))
        out_root, out_ext = os.path.splitext(outfile)
        map_pspace_hdd_backed(
            fn, pspace, out_root + '.part' + out_ext, store=self.store,
            return_data=False)
        os.rename(out_root + '.part' + out_ext, outfile)
