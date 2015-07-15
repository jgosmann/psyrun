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


def append_to_results(data, filename, nodename='/psyrun'):
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
                    # FIXME use save name for tmp node
                    new_node = h5.create_earray(
                        '/tmp', k, atom=tables.Atom.from_dtype(v.dtype),
                        shape=(0,) + shape, createparents=True)
                    for row in node.read():
                        new_node.append(_match_shape([row], shape))
                    new_node.append(v)
                    h5.move_node('/tmp', nodename, k, k, overwrite=True)


def _min_shape(args):
    return tuple(max(x) for x in zip(*args))


def _match_shape(a, shape):
    a = np.asarray(a)
    if a.shape[1:] == shape:
        return a

    matched = np.empty((a.shape[0],) + shape)  # TODO: If float dtype use smallest possible one
    matched.fill(np.nan)

    matched[np.ix_(*(range(x) for x in a.shape))] = a
    return matched
