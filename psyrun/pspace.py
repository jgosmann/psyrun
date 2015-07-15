"""Construction of parameter spaces."""

import itertools

import numpy as np

from psyrun.dict import dict_concat


class _PSpaceObj(object):
    """Abstract base class for objects representing a parameter space.

    Supports addition, subtraction, and multiplication operators to construct
    more complicated parameter spaces.

    Parameters
    ----------
    keys : sequence of strings
        Parameter names.
    """

    def __init__(self, keys):
        self._keys = list(keys)

    def build(self):
        """Builds the parameter space into a dictionary of parameter lists.

        Returns
        -------
        dict
            A dictionary with the parameter names as keys and lists with the
            parameter values.

        Examples
        --------
        >>> from pprint import pprint
        >>> pprint(Param(a=[1, 2], b=[1, 2]).build())
        {'a': [1, 2], 'b': [1, 2]}
        """
        built = dict_concat(list(self.iterate()))
        for k in self.keys():
            built.setdefault(k, [])
        return built

    def iterate(self):
        """Iterates over the parameter assignments in the parameter space."""
        raise NotImplementedError()

    def keys(self):
        """Returns the parameter names."""
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
    """Constructs a simple parameter space from constructor arguments.

    Supports addition, subtraction, and multiplication operators to construct
    more complicated parameter spaces.

    Parameters
    ----------
    params : scalar or sequence
        Each keyword argument defines a parameter with a sequence of parameter
        values for it. The length of all lists has to be equal. If a scalar
        instead of a sequence is passed in, it will be replicated to match the
        length of the other parameters. At least on keyword argument has to be
        a sequence.
    """
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
        """Iterates over the parameter assignments in the parameter space."""
        for i in range(len(self)):
            yield {k: self.get_param(k, i) for k in self.keys()}

    def __len__(self):
        return self._len

    def get_param(self, key, i):
        """Return the i-th parameter assignment.

        Parameters
        ----------
        key : str
            Parameter name of parameter to retrieve.
        i : int
            Index of assigned value to return.
        """
        p = self._params[key]
        try:
            return p[i]
        except TypeError:
            return p


class Difference(_PSpaceObj):
    """Implements the difference of two parameter spaces.

    Parameters
    ----------
    left : _PSpaceObj
        Left operand.
    right : _PSpaceObj
        Right operand.

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(Difference(Param(a=[1, 2], b=[1, 2]), Param(a=[1])).build())
    {'a': [2], 'b': [2]}
    """
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
        """Iterates over the parameter assignments in the parameter space."""
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
    """Implements the outer product of two parameter spaces.

    Parameters
    ----------
    left : _PSpaceObj
        Left operand.
    right : _PSpaceObj
        Right operand.

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(Product(Param(a=[1, 2]), Param(b=[1, 2])).build())
    {'a': [1, 1, 2, 2], 'b': [1, 2, 1, 2]}
    """
    def __init__(self, left, right):
        shared_keys = set(left.keys()).intersection(set(right.keys()))
        if len(shared_keys) > 0:
            raise AmbiguousOperationError(
                'Duplicate param keys: {0}'.format(shared_keys))

        super(Product, self).__init__(list(left.keys()) + list(right.keys()))
        self.left = left
        self.right = right

    def iterate(self):
        """Iterates over the parameter assignments in the parameter space."""
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
    """Implements the concatenation of two parameter spaces.

    Parameters
    ----------
    left : _PSpaceObj
        Left operand.
    right : _PSpaceObj
        Right operand.

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(Sum(Param(a=[1]), Param(a=[2])).build())
    {'a': [1, 2]}
    """
    def __init__(self, left, right):
        super(Sum, self).__init__(set(left.keys()).union(set(right.keys())))
        self.left = left
        self.right = right

    def iterate(self):
        """Iterates over the parameter assignments in the parameter space."""
        return ({k: item.get(k, np.nan) for k in self.keys()}
                for item in itertools.chain(
                    self.left.iterate(), self.right.iterate()))

    def __len__(self):
        return len(self.left) + len(self.right)


class AmbiguousOperationError(RuntimeError):
    """Two parameter space were tried to combined in an ambiguous way."""
    pass
