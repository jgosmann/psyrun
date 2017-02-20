"""Backends to store load and store data."""

import errno

# FIXME shouldn't be a dependency?
import numpy as np
from six import string_types
from six.moves import cPickle as pickle


class AbstractStore(object):
    """Base class for classes implementing a store.

    Attributes
    ----------
    ext : str
        Filename extension used by the store.
    """

    ext = ''

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


class PickleStore(AbstractStore):
    """Store using Python pickle `.pkl` files."""

    ext = '.pkl'

    def __init__(self, protocol=pickle.HIGHEST_PROTOCOL):
        self.protocol = protocol

    def save(self, filename, data):
        with open(filename, 'wb') as f:
            pickle.dump(data, f, self.protocol)

    def load(self, filename, row=None):
        with open(filename, 'rb') as f:
            data = pickle.load(f)
        if row is None:
            return data
        else:
            return {k: [v[row]] for k, v in data.items()}

    def append(self, filename, data):
        try:
            loaded = self.load(filename)
        except IOError as err:
            if err.errno != errno.ENOENT:
                raise
            self.save(filename, data)
        else:
            keys = set(loaded.keys())
            keys = keys.union(data.keys())

            for k in keys:
                if k not in loaded:
                    n = max(len(v) for v in loaded.values())
                    loaded[k] = [None] * n
                if not isinstance(loaded[k], list):
                    loaded[k] = list(loaded[k])
                v = data.get(k, [None])
                if isinstance(v, string_types + (bytes,)):
                    v = [v]
                loaded[k].extend(v)

            self.save(filename, loaded)


class NpzStore(AbstractStore):
    """Store using NumPy `.npz` files.

    This backend needs to load all data in a file to append to it. Similarly,
    individual rows from a data file can only be returned by loading the whole
    file. Thus, this backend is not recommended to be used with applications
    that produce large amounts of data or with load balancing if the parameter
    space is large.
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
            shape = _min_shape(np.asarray(x).shape for x in v)
            try:
                node = loaded[k]
            except KeyError:
                v = _match_shape(v, shape)
                loaded[k] = v
            else:
                shape = _min_shape((shape, node.shape[1:]))
                loaded[k] = _match_shape(loaded[k], shape)
                v = _match_shape(v, shape)
                loaded[k] = np.concatenate((loaded[k], v))
        self.save(filename, loaded)


class H5Store(AbstractStore):
    """Store using the HDF5 format.

    This store allows for efficient appending as long as the dimensions match.

    Requires pytables to be installed.

    Parameters
    ----------
    node : str, optional
        Node in the HDF5 file to store the data at.
    """

    ext = '.h5'

    def __init__(self, node='/psyrun'):
        super(H5Store, self).__init__()
        self.node = node

    def save(self, filename, data):
        import tables
        with tables.open_file(filename, 'w') as h5:
            for k, v in data.items():
                self._ensure_supported(v)
                h5.create_array(self.node, k, v, createparents=True)

    def load(self, filename, row=None):
        import tables
        with tables.open_file(filename, 'r') as h5:
            return {node._v_name: node.read(row) for node in h5.iter_nodes(
                self.node)}

    def append(self, filename, data):
        import tables
        with tables.open_file(filename, 'a') as h5:
            for k, v in data.items():
                self._ensure_supported(v)
                shape = _min_shape(np.asarray(x).shape for x in v)
                try:
                    node = h5.get_node(self.node, k)
                except tables.NoSuchNodeError:
                    v = _match_shape(v, shape)
                    h5.create_earray(
                        self.node, k, obj=v, shape=(0,) + shape,
                        createparents=True)
                else:
                    shape = _min_shape((shape, node.shape[1:]))
                    v = _match_shape(v, shape)
                    if (shape == node.shape[1:] and
                            not isinstance(node.atom, tables.StringAtom)):
                        node.append(v)
                    else:
                        tmp_node = self._get_tmp_node_name(h5)
                        new_node = h5.create_earray(
                            tmp_node, k, atom=tables.Atom.from_dtype(v.dtype),
                            shape=(0,) + shape, createparents=True)
                        for row in node.read():
                            new_node.append(_match_shape([row], shape))
                        new_node.append(v)
                        h5.move_node(tmp_node, self.node, k, k, overwrite=True)

    @classmethod
    def _ensure_supported(cls, v):
        dtype = np.asarray(v).dtype
        if dtype.kind in 'OSU':
            raise NotImplementedError(
                "H5Store does not support dtype {}.".format(dtype))

    @staticmethod
    def _get_tmp_node_name(h5):
        i = 0
        while '/tmp{0}'.format(i) in h5:
            i += 1
        return '/tmp{0}'.format(i)


def _min_shape(args):
    return tuple(max(x) for x in zip(*args))


def _match_shape(a, shape):
    a = np.asarray(a)
    if a.shape == ():
        a = np.asarray([a])
    if a.shape[1:] == shape:
        return a

    dtype = a.dtype
    matched = np.empty((a.shape[0],) + shape, dtype=dtype)
    if np.issubdtype(dtype, float) or np.issubdtype(dtype, complex):
        matched.fill(np.nan)
    elif dtype.kind == 'S':  # FIXME bytes and unicode?
        matched.fill('')
    # FIXME warning if no nan supported but missing values

    matched[np.ix_(*(range(x) for x in a.shape))] = a
    return matched
