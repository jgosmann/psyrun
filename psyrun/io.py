import numpy as np
import tables

from psyrun.pspace import Param


def save_infile(pspace, infile):
    with tables.open_file(infile, 'w') as h5:
        for k, v in pspace.items():
            h5.create_array('/pspace', k, v, createparents=True)


def load_infile(infile):
    with tables.open_file(infile, 'r') as h5:
        return Param(
            **{node._v_name: node.read() for node in h5.iter_nodes('/pspace')})


def save_outfile(data, outfile):
    with tables.open_file(outfile, 'w') as h5:
        for k, v in data.items():
            h5.create_array('/data', k, v, createparents=True)


def load_results(filename):
    with tables.open_file(filename, 'r') as h5:
        return {node._v_name: node.read() for node in h5.iter_nodes('/data')}


def append_to_results(data, filename):

    with tables.open_file(filename, 'a') as h5:

        for k, v in data.items():
            shape = _min_shape(np.asarray(x).shape for x in v)
            try:
                node = h5.get_node('/data', k)
            except tables.NoSuchNodeError:
                v = _match_shape(v, shape)
                h5.create_earray(
                    '/data', k, obj=v, shape=(0,) + shape, createparents=True)
            else:
                shape = _min_shape((shape, node.shape[1:]))
                v = _match_shape(v, shape)
                if shape == node.shape[1:]:
                    node.append(v)
                else:
                    new_node = h5.create_earray(
                        '/tmp', k, atom=tables.Atom.from_dtype(v.dtype),
                        shape=(0,) + shape, createparents=True)
                    for row in node.read():
                        new_node.append(_match_shape([row], shape))
                    new_node.append(v)
                    h5.move_node('/tmp', '/data', k, k, overwrite=True)


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
