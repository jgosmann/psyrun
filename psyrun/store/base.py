"""Base store interface."""

import os.path
from pkg_resources import iter_entry_points


class Store(object):
    """Defines the interface of stores.

    Register implemented stores as
    `entry points <https://setuptools.readthedocs.io/en/latest/setuptools.html#dynamic-discovery-of-services-and-plugins>`_
    in the ``psyrun.stores`` groupn. For example, add the following to the
    ``setup`` call in your store's ``setup.py`` for a store providing the
    ``.ext`` format::

        entry_points={
            'psyrun.stores': ['.ext = pkg.name:ClassName'],
        }

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


def _safe_ep_load():
    for ep in iter_entry_points('psyrun.stores'):
        try:
            yield ep.name, ep.load()
        except ImportError:
            pass


class AutodetectStore(Store):
    """Automatically selects the store based on the file extension."""


    registry = dict(_safe_ep_load())

    @classmethod
    def get_concrete_store(cls, filename):
        _, ext = os.path.splitext(filename)
        return cls.registry[ext.lower()]()

    def append(self, filename, data):
        return self.get_concrete_store(filename).append(filename, data)

    def save(self, filename, data):
        return self.get_concrete_store(filename).save(filename, data)

    def load(self, filename, row=None):
        return self.get_concrete_store(filename).load(filename, row=row)
