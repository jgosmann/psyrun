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
            try:
                node = h5.get_node('/data', k)
            except tables.NoSuchNodeError:
                h5.create_earray('/data', k, obj=v, createparents=True)
            else:
                node.append(v)
