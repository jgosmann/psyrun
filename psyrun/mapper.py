"""Map functions onto parameter spaces."""

import os.path

from psyrun.pspace import dict_concat, missing, Param


def get_result(fn, params):
    """Evaluates a function with given parameters.

    Evaluates *fn* with the parameters *param* and returns a dictionary with
    the input parameters and returned output values.

    Parameters
    ----------
    fn : function
        Function to evaluate. Has to return a dictionary.
    params : dict
        Parameters passed to *fn* as keyword arguments.

    Returns
    -------
    dict
        Returns *params* updated with the return value of *fn*.

    Examples
    --------
    >>> def fn(x, is_result):
    ...     return {'y': x * x, 'is_result': 1}
    >>>
    >>> from pprint import pprint
    >>> pprint(get_result(fn, {'x': 4, 'is_result': 0}))
    {'is_result': 1, 'x': 4, 'y': 16}
    """
    result = dict(params)
    result.update(fn(**params))
    return result


def map_pspace(fn, pspace):
    """Maps a function to parameter space values.

    Parameters
    ----------
    fn : function
        Function to evaluate on parameter space. Has to return a dictionary.
    pspace : `ParameterSpace`
        Parameter space providing parameter values to evaluate function on.

    Returns
    -------
    dict
        Dictionary with the input parameter values and the function return
        values.

    Examples
    --------
    >>> def fn(x):
    ...     return {'y': x * x}
    >>>
    >>> from pprint import pprint
    >>> from psyrun import Param
    >>> pprint(map_pspace(fn, Param(x=[1, 2])))
    {'x': [1, 2], 'y': [1, 4]}
    """
    return dict_concat(list(get_result(fn, p) for p in pspace.iterate()))


def map_pspace_hdd_backed(fn, pspace, filename, store, return_data=True):
    """Maps a function to parameter space values while storing produced data.

    Data is stored progressively. Thus, if the program crashes, not all data
    will be lost.

    Parameters
    ----------
    fn : function
        Function to evaluate on parameter space. Has to return a dictionary.
    pspace : `ParameterSpace`
        Parameter space providing parameter values to evaluate function on.
    filename : str
        Filename of file to store data to.
    store : `Store`
        Store to save data with.
    return_data : bool, optional
        Whether to return the resulting data after mapping the function. This
        will read all produced data from the disk.

    Returns
    -------
    None or dict
        Dictionary with the input parameter values and the function return
        values if requested.
    """
    if os.path.exists(filename):
        pspace = missing(pspace, Param(**store.load(filename)))
    for p in pspace.iterate():
        store.append(filename, dict_concat((get_result(fn, p),)))
    if not os.path.exists(filename):
        store.save(filename, {})
    if return_data:
        return store.load(filename)


def map_pspace_parallel(fn, pspace, n_jobs=-1, backend='multiprocessing'):
    """Maps a function to parameter space values in parallel.

    Requires `joblib <https://pythonhosted.org/joblib/>`_.

    Parameters
    ----------
    fn : function
        Function to evaluate on parameter space. Has to return a dictionary.
    pspace : ParameterSpace
        Parameter space providing parameter values to evaluate function on.
    n_jobs : int, optional
        Number of parallel jobs. Set to -1 to automatically determine.
    backend : str, optional
        Backend to use. See `joblib documentation
        <https://pythonhosted.org/joblib/parallel.html#using-the-threading-backend>`_
        for details.

    Returns
    -------
    dict
        Dictionary with the input parameter values and the function return
        values.

    Examples
    --------
    >>> from pprint import pprint
    >>> from psyrun import Param
    >>> from psyrun.utils.example import square
    >>>
    >>> pprint(map_pspace_parallel(square, Param(x=[1, 2])))
    {'x': [1, 2], 'y': [1, 4]}
    """
    import joblib
    parallel = joblib.Parallel(n_jobs=n_jobs, backend=backend)
    return dict_concat(parallel(
        joblib.delayed(get_result)(fn, p) for p in pspace.iterate()))
