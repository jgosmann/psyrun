"""HDF5 store."""

import numpy as np
import tables

from psyrun.store.base import Store
from psyrun.store.np_shape import min_shape, match_shape
from psyrun.utils.doc import inherit_docs


@inherit_docs
class H5Store(Store):
    """Store using the HDF5 format.

    This store allows for efficient appending as long as the dimensions match.

    Requires `NumPy <http://www.numpy.org/>`_ and
    `PyTables <http://www.pytables.org/>`_ . Only numeric data types are
    supported.

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
        with tables.open_file(filename, 'w') as h5:
            for k, v in data.items():
                self._ensure_supported(v)
                h5.create_array(self.node, k, v, createparents=True)

    def load(self, filename, row=None):
        with tables.open_file(filename, 'r') as h5:
            return {node._v_name: node.read(row) for node in h5.iter_nodes(
                self.node)}

    def append(self, filename, data):
        with tables.open_file(filename, 'a') as h5:
            for k, v in data.items():
                self._ensure_supported(v)
                shape = min_shape(np.asarray(x).shape for x in v)
                try:
                    node = h5.get_node(self.node, k)
                except tables.NoSuchNodeError:
                    v = match_shape(v, shape)
                    h5.create_earray(
                        self.node, k, obj=v, shape=(0,) + shape,
                        createparents=True)
                else:
                    shape = min_shape((shape, node.shape[1:]))
                    v = match_shape(v, shape)
                    if (shape == node.shape[1:] and
                            not isinstance(node.atom, tables.StringAtom)):
                        node.append(v)
                    else:
                        tmp_node = self._get_tmp_node_name(h5)
                        new_node = h5.create_earray(
                            tmp_node, k, atom=tables.Atom.from_dtype(v.dtype),
                            shape=(0,) + shape, createparents=True)
                        for row in node.read():
                            new_node.append(match_shape([row], shape))
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
