"""Store using pickle files."""

import errno

from six import string_types
from six.moves import cPickle as pickle

from psyrun.store.base import Store
from psyrun.utils.doc import inherit_docs


@inherit_docs
class PickleStore(Store):
    """Store using Python pickle *.pkl* files.

    It supports all pickle-able data types and has no additional dependencies,
    but is not the most efficient store. Also, it has to load the complete
    file to append to it.
    """

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

    def _get_data_len(self, data):
        if len(data) > 0:
            return max(
                len(v) if not isinstance(v, string_types + (bytes,)) else 1
                for v in data.values())
        else:
            return 0

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

            old_n = self._get_data_len(loaded)
            new_n = self._get_data_len(data)

            for k in keys:
                if k not in loaded:
                    loaded[k] = [None] * old_n
                if not isinstance(loaded[k], list):
                    loaded[k] = list(loaded[k])

                v = data.get(k, [None])
                if len(v) != new_n:
                    if len(v) == 1:
                        v = v * new_n
                    else:
                        raise ValueError("Incompatible data length.")
                loaded[k].extend(v)

            self.save(filename, loaded)
