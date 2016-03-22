"""Function to read and write common psyrun data formats."""

import numpy as np
import tables


class DictStore(object):
    def save(self, filename, data):
        raise NotImplementedError()

    def load(self, filename):
        raise NotImplementedError()

    def append(self, filename, data):
        raise NotImplementedError()


class H5Store(DictStore):
    """Store and access dictionaries in HDF5 files.

    Parameters
    ==========
    node : str, optional
        Node in the HDF5 file to store the data at.
    """

    def __init__(self, node='/psyrun'):
        super(H5Store, self).__init__()
        self.node = node

    def save(self, filename, data):
        """Save a dictionary in an HDF5 file.

        Parameters
        ----------
        filename : str
            Filename of file to write to.
        data : dict
            Data to store.
        """
        with tables.open_file(filename, 'w') as h5:
            for k, v in data.items():
                h5.create_array(self.node, k, v, createparents=True)

    def load(self, filename):
        """Read a dictionary from an HDF5 file.

        Parameters
        ----------
        filename : str
            Filename of the file to read from.

        Returns
        -------
        dict
            The data loaded.
        """
        with tables.open_file(filename, 'r') as h5:
            return {node._v_name: node.read() for node in h5.iter_nodes(
                self.node)}


    def append(self, filename, data):
        """Appends to a saved dictionary in an HDF5 file.

        The shape of already stored data and the new data will be padded with
        ``nan`` as necessary to make the shapes match. If the stored data has
        to be padded, this will load all of the stored data into memory.

        Parameters
        ----------
        filename : str
            Filename of file to write to.
        data : dict
            Data to store.
        """
        with tables.open_file(filename, 'a') as h5:
            for k, v in data.items():
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
                    if shape == node.shape[1:]:
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
    if a.shape[1:] == shape:
        return a

    dtype = a.dtype
    if not np.issubdtype(dtype, float):
        dtype = float
    matched = np.empty((a.shape[0],) + shape, dtype=dtype)
    matched.fill(np.nan)

    matched[np.ix_(*(range(x) for x in a.shape))] = a
    return matched
