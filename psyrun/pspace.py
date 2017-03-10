"""Construction of parameter spaces."""

import collections
import itertools

from six import string_types

from psyrun.utils.doc import inherit_docs


def dict_concat(args):
    """Concatenates elements with the same key in the passed dictionaries.

    Parameters
    ----------
    args : sequenece of dict
        Dictionaries with sequences to concatenate.

    Returns
    -------
    dict
        The dictionary with the union of all the keys of the dictionaries
        passed in and elements with the same key concatenated. Missing elements
        will be None.

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(dict_concat(({'a': 0, 'b': 0}, {'a': 1})))
    {'a': [0, 1], 'b': [0, None]}
    """
    keys = set()
    for a in args:
        keys = keys.union(a.keys())
    return {k: [a.get(k, None) for a in args] for k in keys}


class ParameterSpace(collections.Sized):
    """Abstract base class for objects representing a parameter space.

    Supports addition, subtraction, and multiplication operators to construct
    more complicated parameter spaces.

    Deriving classes are supposed to implement the `iterate` and ``__len__``
    methods.


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

    def __repr__(self):
        keys = sorted(self.keys())
        built = self.build()
        return "Param(**{{{params}}})".format(
            params=", ".join(
                "{k!r}: {v!r}".format(k=k, v=built[k]) for k in keys))

    def __str__(self):
        keys = sorted(self.keys())
        built = self.build()
        return "Param({params})".format(
            params=", ".join(
                "{k!s}={v!r}".format(k=k, v=built[k]) for k in keys))


@inherit_docs
class Param(ParameterSpace):
    """Constructs a simple parameter space from constructor arguments.

    Supports addition, subtraction, and multiplication operators to construct
    more complicated parameter spaces.

    Parameters
    ----------
    params :
        Each keyword argument defines a parameter with a sequence of parameter
        values for it. The length of all lists has to be equal. If a scalar
        instead of a sequence is passed in, it will be replicated to match the
        length of the other parameters. At least one keyword argument has to be
        a sequence.
    """
    def __init__(self, **params):
        super(Param, self).__init__(params.keys())
        self._params = params

        # Make sure strings etc are in a list.
        for k in self._params:
            if isinstance(self._params[k], string_types + (bytes,)):
                self._params[k] = [self._params[k]]

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
        """Return the *i*-th parameter assignment.

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


@inherit_docs
class Difference(ParameterSpace):
    """Implements the difference of two parameter spaces.

    Parameters
    ----------
    minuend : `ParameterSpace`
        Minuend (left operand).
    subtrahend : `ParameterSpace`
        Subtrahend (right operand).

    Attributes
    ----------
    minuend : `ParameterSpace`
        Minuend (left operand).
    subtrahend : `ParameterSpace`
        Subtrahend (right operand).

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(Difference(Param(a=[1, 2], b=[1, 2]), Param(a=[1])).build())
    {'a': [2], 'b': [2]}
    """

    def __init__(self, minuend, subtrahend):
        super(Difference, self).__init__(minuend.keys())
        for k in subtrahend.keys():
            if k not in self._keys:
                raise AmbiguousOperationError(
                    'Key `{0}` not existent in minuend.'.format(k))
        self.left = minuend
        self.right = subtrahend
        self._cached = None

    def iterate(self):
        if len(self.right) == 0:
            return self.left.iterate()
        if self._cached is None:
            exclude = self.right.build()
            self._cached = [item for item in self.left.iterate()
                            if not all(item[k] in exclude[k]
                                       for k in exclude.keys())]
        return iter(self._cached)

    def __len__(self):
        return sum(1 for item in self.iterate())


@inherit_docs
class Product(ParameterSpace):
    """Implements the Cartesian product of two parameter spaces.

    Parameters
    ----------
    left : `ParameterSpace`
        Left operand.
    right : `ParameterSpace`
        Right operand.

    Attributes
    ----------
    left : `ParameterSpace`
        Left operand.
    right : `ParameterSpace`
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


@inherit_docs
class Sum(ParameterSpace):
    """Implements the concatenation of two parameter spaces.

    Parameters
    ----------
    left : `ParameterSpace`
        Left operand.
    right : `ParameterSpace`
        Right operand.

    Attributes
    ----------
    left : `ParameterSpace`
        Left operand.
    right : `ParameterSpace`
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
        return ({k: item.get(k, float('nan')) for k in self.keys()}
                for item in itertools.chain(
                    self.left.iterate(), self.right.iterate()))

    def __len__(self):
        return len(self.left) + len(self.right)


def missing(minuend, subtrahend):
    """Return set of parameter assignments missing from another set.

    This differs from a simple subtraction by allowing additional keys in the
    subtrahend, but no additional keys in the minuend.

    Parameters
    ----------
    minuend : `ParameterSpace`
        Parameter space with all assignments.
    subtrahend : :class:`Param`
        Parameter space with assignments to remove from the parameter space.

    Returns
    -------
    `ParameterSpace`
        The reduced parameter space.

    Examples
    --------
    >>> from pprint import pprint
    >>> pprint(missing(Param(a=[1, 2, 3]), Param(a=[2])).build())
    {'a': [1, 3]}
    """
    if len(subtrahend) <= 0:
        return minuend
    for k in minuend.keys():
        if k not in subtrahend.keys():
            raise AmbiguousOperationError()
    return minuend - Param(
        **{k: v for k, v in subtrahend.build().items() if k in minuend.keys()})


class AmbiguousOperationError(RuntimeError):
    """Attempt to combine two parameter spaces in an ambiguous way."""
    pass
