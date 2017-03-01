"""NumPy NPZ store."""

import errno

import numpy as np

from psyrun.store.base import Store
from psyrun.store.np_shape import min_shape, match_shape
from psyrun.utils.doc import inherit_docs


@inherit_docs
class NpzStore(Store):
    """Store using NumPy *.npz* files.

    Requires `NumPy <http://www.numpy.org/>`_.

    This store needs to load all data in a file to append to it. Similarly,
    individual rows from a data file can only be returned by loading the whole
    file. Thus, this store is not recommended to be used with applications
    that produce large amounts of data or with the `LoadBalancingBackend` if
    the parameter space is large. It is however more efficient than the
    `PickleStore`.
    """

    ext = '.npz'

    def save(self, filename, data):
        np.savez(filename, **data)

    def load(self, filename, row=None):
        try:
            with np.load(filename) as data:
                if row is None:
                    return dict(data)
                else:
                    return {k: [v[row]] for k, v in data.items()}
        except IOError as err:
            if 'as a pickle' in str(err):
                return {}
            else:
                raise

    def append(self, filename, data):
        try:
            loaded = self.load(filename)
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            loaded = {}

        for k, v in data.items():
            shape = min_shape(np.asarray(x).shape for x in v)
            try:
                node = loaded[k]
            except KeyError:
                v = match_shape(v, shape)
                loaded[k] = v
            else:
                shape = min_shape((shape, node.shape[1:]))
                loaded[k] = match_shape(loaded[k], shape)
                v = match_shape(v, shape)
                loaded[k] = np.concatenate((loaded[k], v))
        self.save(filename, loaded)
