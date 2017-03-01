"""Base store interface."""


class Store(object):
    """Defines the interface of stores.

    Attributes
    ----------
    ext : str
        Filename extension used by the store.
    """

    ext = ''

    def append(self, filename, data):
        """Append data to file.

        When trying to append data to a non-existing file, a new file will be
        created. The backend may require that a file was created with this
        function to be able to append to it.

        Parameters
        ----------
        filename : str
            Filename of file to append the data to.
        data : dict
            Dictionary with data to append.
        """
        raise NotImplementedError()

    def save(self, filename, data):
        """Save data to a file.

        Parameters
        ----------
        filename : str
            Filename of file to save data to.
        data : dict
            Dictionary with data to store.
        """
        raise NotImplementedError()

    def load(self, filename, row=None):
        """Load data from a file.

        Parameters
        ----------
        filename : str
            Filename of file to load data from.
        row : int, optional
            If given, only the row with this index will be loaded.

        Returns
        -------
        dict
            Loaded data.
        """
        raise NotImplementedError()
