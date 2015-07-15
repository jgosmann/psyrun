from psyrun.io import load_infile, save_outfile


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
        pspace = load_infile(infile)
        data = self.mapper(fn, pspace, **self.mapper_kwargs)
        save_outfile(data, outfile)
