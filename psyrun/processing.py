"""File-based processing of parameter spaces."""

import os
import os.path

from psyrun.io import append_dict_h5, load_dict_h5, save_dict_h5
from psyrun.pspace import dict_concat, Param


class Splitter(object):
    """Split a parameter space into multiple input files and merge results
    after processing.

    Parameters
    ----------
    workdir : str
        Working directory to create input files in and read output files from.
    pspace : :class:`._PSpaceObj`
        Parameter space to split up.
    max_splits : int
        Maximum number of splits to perform.
    min_items : int
        Minimum number of parameter sets in each split.
    """
    def __init__(self, workdir, pspace, max_splits=64, min_items=4):
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

    @property
    def n_splits(self):
        """Number of total splits that will be generated."""
        return min(
            self.max_splits, (len(self.pspace) - 1) // self.min_items + 1)

    def split(self):
        """Perform splitting of parameters space and save input files for
        processing."""
        items_remaining = len(self.pspace)
        param_iter = self.pspace.iterate()
        for i, filename in enumerate(self._iter_filenames()):
            split_size = max(
                self.min_items, items_remaining // (self.max_splits - i))
            items_remaining -= split_size
            block = dict_concat(
                [row for row in self._iter_n(param_iter, split_size)])
            save_dict_h5(os.path.join(self.indir, filename), block)

    @classmethod
    def merge(cls, outdir, merged_filename, append=True):
        """Merge processed files together.

        Parameters
        ----------
        outdir : str
            Directory with the output files.
        merged_filename : str
            Filename of file to save with the merged results.
        append : bool
            If ``True`` the merged data will be appended, otherwise the file
            will be overwritten with the merged data.
        """
        if not append:
            save_dict_h5(merged_filename, {})
        for filename in os.listdir(outdir):
            if os.path.splitext(filename)[1] != '.h5':
                continue
            infile = os.path.join(outdir, filename)
            append_dict_h5(merged_filename, load_dict_h5(infile))

    def iter_in_out_files(self):
        """Return generator returning tuples of corresponding input and output
        filenames."""
        return ((os.path.join(self.indir, f), os.path.join(self.outdir, f))
                for f in self._iter_filenames())

    def _iter_filenames(self):
        return ('{0}.h5'.format(i) for i in range(self.n_splits))

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
    mapper : function
        Function that takes another function, a parameter space, and
        potentially further keyword arguments and returns the result of mapping
        the function onto the parameter space.
    mapper_kwargs : dict
        Additional keyword arguments to pass to the `mapper`.
    """

    def __init__(self, mapper, **mapper_kwargs):
        self.mapper = mapper
        self.mapper_kwargs = mapper_kwargs

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
        pspace = Param(**load_dict_h5(infile))
        data = self.mapper(fn, pspace, **self.mapper_kwargs)
        save_dict_h5(outfile, data)
