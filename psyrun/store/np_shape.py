"""NumPy shape handling utility functions."""

import numpy as np


def min_shape(args):
    """Returns the minimum shape that encompasses the shape of all *args*."""
    args = tuple(args)
    if len(args) <= 0:
        raise ValueError("args must not be empty.")
    elif not all(len(a) == len(args[0]) for a in args):
        raise ValueError("All shapes must have the same number of dimensions.")
    return tuple(max(x) for x in zip(*args))


def match_shape(a, shape):
    """Make *a* match the shape *(a.shape[0],) + shape*."""

    a = np.asarray(a)
    if a.shape == ():
        a = np.asarray([a])
    if a.shape[1:] == shape:
        return a

    dtype = a.dtype
    matched = np.empty((a.shape[0],) + shape, dtype=dtype)
    if np.issubdtype(dtype, np.inexact):
        matched.fill(np.nan)
    elif np.issubdtype(dtype, np.number):
        matched = np.asfarray(matched)
        matched.fill(np.nan)
    elif dtype.kind in 'SU':
        matched.fill('')
    elif dtype.kind == 'O':
        matched.fill(None)
    else:
        raise ValueError(
            "Dtype {} does not support missing values.".format(dtype))

    matched[np.ix_(*(range(x) for x in a.shape))] = a
    return matched
