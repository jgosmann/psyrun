"""NumPy shape handling utility functions."""

import numpy as np


def min_shape(args):
    """Returns the minimum shape that encompasses the shape of all *args*."""
    return tuple(max(x) for x in zip(*args))


def match_shape(a, shape):
    """Make *a* match the shape *shape*."""

    a = np.asarray(a)
    if a.shape == ():
        a = np.asarray([a])
    if a.shape[1:] == shape:
        return a

    dtype = a.dtype
    matched = np.empty((a.shape[0],) + shape, dtype=dtype)
    if np.issubdtype(dtype, float) or np.issubdtype(dtype, complex):
        matched.fill(np.nan)
    elif dtype.kind in 'SU':
        matched.fill('')
    elif dtype.kind == 'O':
        matched.fill(None)
    else:
        raise ValueError("Dtype does not support missing values.")

    matched[np.ix_(*(range(x) for x in a.shape))] = a
    return matched
