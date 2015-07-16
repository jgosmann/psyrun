"""Function to read and write common psyrun data formats."""

import numpy as np
import tables


def save_dict_h5(filename, data, node='/psyrun', mode='w'):
    """Save a dictionary in an HDF5 file.

    Parameters
    ----------
    filename : str
        Filename of file to write to.
    data : dict
        Data to store.
    node : str, optional
        Node in the HDF5 file to store the data at.
    mode : str, optional
        The open mode of the file. Can be ``'w'`` to create a new file and
        overwrite existing files, ``'a'`` to add to an existing file, and
        ``'r+'`` to append to an existing file which is required to already
        exist.
    """
    with tables.open_file(filename, mode) as h5:
        for k, v in data.items():
            h5.create_array(node, k, v, createparents=True)


def load_dict_h5(filename, node='/psyrun'):
    """Read a dictionary from an HDF5 file.

    Parameters
    ----------
    filename : str
        Filename of the file to read from.
    node : str, optional
        Node in the HDF5 file where the data is stored.

    Returns
    -------
    dict
        The data loaded.
    """
    with tables.open_file(filename, 'r') as h5:
        return {node._v_name: node.read() for node in h5.iter_nodes(node)}


def append_dict_h5(filename, data, nodename='/psyrun'):
    """Appends to a saved dictionary in an HDF5 file.

    The shape of already stored data and the new data will be padded with
    ``nan`` as necessary to make the shapes match. If the stored data has to be
    padded, this will load all of the stored data into memory.

    Parameters
    ----------
    filename : str
        Filename of file to write to.
    data : dict
        Data to store.
    node : str, optional
        Node in the HDF5 file to store the data at.
    """
    with tables.open_file(filename, 'a') as h5:
        for k, v in data.items():
            shape = _min_shape(np.asarray(x).shape for x in v)
            try:
                node = h5.get_node(nodename, k)
            except tables.NoSuchNodeError:
                v = _match_shape(v, shape)
                h5.create_earray(
                    nodename, k, obj=v, shape=(0,) + shape, createparents=True)
            else:
                shape = _min_shape((shape, node.shape[1:]))
                v = _match_shape(v, shape)
                if shape == node.shape[1:]:
                    node.append(v)
                else:
                    tmp_node = _get_tmp_node_name(h5)
                    new_node = h5.create_earray(
                        tmp_node, k, atom=tables.Atom.from_dtype(v.dtype),
                        shape=(0,) + shape, createparents=True)
                    for row in node.read():
                        new_node.append(_match_shape([row], shape))
                    new_node.append(v)
                    h5.move_node(tmp_node, nodename, k, k, overwrite=True)


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


def _get_tmp_node_name(h5):
    i = 0
    while '/tmp{0}'.format(i) in h5:
        i += 1
    return '/tmp{0}'.format(i)
