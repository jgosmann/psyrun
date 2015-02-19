import itertools

import pandas as pd


def _merge(left, right):
    assert len(left) == len(right)
    left.reset_index(drop=True, inplace=True)
    right.reset_index(drop=True, inplace=True)
    return pd.merge(left, right, left_index=True, right_index=True)


class _PSpaceObj(object):
    def __init__(self, keys):
        self._keys = keys

    def build(self):
        try:
            return pd.concat(self.iterate())
        except ValueError:
            return pd.DataFrame()

    def iterate(self):
        raise NotImplementedError()

    def keys(self):
        return self._keys

    def __add__(self, other):
        return Sum(self, other)

    def __mul__(self, other):
        return Product(self, other)

    def __sub__(self, other):
        return Difference(self, other)


class Param(_PSpaceObj):
    def __init__(self, **params):
        super(Param, self).__init__(params.keys())
        self._params = pd.DataFrame(params)

    def build(self):
        return pd.DataFrame(self._params)

    def iterate(self):
        for i in range(len(self._params)):
            yield self._params.iloc[i:i + 1]

    def __len__(self):
        return len(self._params)


class Difference(_PSpaceObj):
    def __init__(self, left, right):
        super(Difference, self).__init__(left.keys())
        for k in right.keys():
            if k not in self._keys:
                raise AmbiguousOperationError(
                    'Key `{0}` not existent in minuend.'.format(k))
        self.left = left
        self.right = right
        self._cached = None

    def iterate(self):
        if len(self.right) == 0:
            return self.left.iterate()
        if self._cached is None:
            exclude = self.right.build().reset_index(drop=True)
            self._cached = (item for item in self.left.iterate()
                if not (exclude == item[exclude.columns].reset_index(
                    drop=True)).all(1).any())
        return self._cached

    def __len__(self):
        return sum(1 for item in self.iterate())


class Product(_PSpaceObj):
    def __init__(self, left, right):
        shared_keys = set(left.keys()).intersection(set(right.keys()))
        if len(shared_keys) > 0:
            raise AmbiguousOperationError(
                'Duplicate param keys: {0}'.format(shared_keys))
        super(Product, self).__init__(list(left.keys()) + list(right.keys()))
        self.left = left
        self.right = right

    def iterate(self):
        if len(self.left.keys()) == 0:
            return self.right.iterate()
        elif len(self.right.keys()) == 0:
            return self.left.iterate()
        else:
            return (_merge(*item) for item in itertools.product(
                self.left.iterate(), self.right.iterate()))

    def __len__(self):
        if len(self.left.keys()) == 0:
            return len(self.right)
        elif len(self.right.keys()) == 0:
            return len(self.left)
        else:
            return len(self.left) * len(self.right)


class Sum(_PSpaceObj):
    def __init__(self, left, right):
        super(Sum, self).__init__(set(left.keys()).union(set(right.keys())))
        self.left = left
        self.right = right

    def iterate(self):
        return (pd.DataFrame(item, columns=self.keys())
                for item in itertools.chain(
                    self.left.iterate(), self.right.iterate()))

    def __len__(self):
        return len(self.left) + len(self.right)


class AmbiguousOperationError(RuntimeError):
    pass
