import itertools

from psyrun.dict import dict_concat


class _PSpaceObj(object):
    def __init__(self, keys):
        self._keys = keys

    def build(self):
        built = dict_concat(list(self.iterate()))
        for k in self.keys():
            built.setdefault(k, [])
        return built

    def iterate(self):
        raise NotImplementedError()

    def keys(self):
        return self._keys

    def __len__(self):
        raise NotImplementedError()

    def __add__(self, other):
        return Sum(self, other)

    def __mul__(self, other):
        return Product(self, other)

    def __sub__(self, other):
        return Difference(self, other)


class Param(_PSpaceObj):
    def __init__(self, **params):
        super(Param, self).__init__(params.keys())
        self._params = params

        self._len = None
        for v in self._params.values():
            try:
                l = len(v)
            except TypeError:
                pass
            else:
                if self._len is None:
                    self._len = l
                elif self._len != l:
                    raise ValueError("Parameter lists differ in length.")
        if self._len is None:
            self._len = 1 if len(self._params) > 0 else 0

    def iterate(self):
        for i in range(len(self)):
            yield {k: self.get_param(k, i) for k in self.keys()}

    def __len__(self):
        return self._len

    def get_param(self, key, i):
        p = self._params[key]
        try:
            return p[i]
        except TypeError:
            return p


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
            exclude = self.right.build()
            self._cached = (item for item in self.left.iterate()
                            if not all(item[k] in exclude[k]
                                       for k in exclude.keys()))
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
            return (self._merge(*item) for item in itertools.product(
                self.left.iterate(), self.right.iterate()))

    @staticmethod
    def _merge(left, right):
        merged = {}
        merged.update(left)
        merged.update(right)
        return merged

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
        return ({k: item.get(k, None) for k in self.keys()}
                for item in itertools.chain(
                    self.left.iterate(), self.right.iterate()))

    def __len__(self):
        return len(self.left) + len(self.right)


class AmbiguousOperationError(RuntimeError):
    pass
