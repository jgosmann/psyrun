.. _guide:

User Guide
==========


Constructing parameter spaces
-----------------------------

Parameter spaces are constructed with the :class:`.Param` class. You pass in
sequences of parameter values as keyword arguments. As long as you pass in at
least one sequence other parameter values are allowed to be scalars and will be
replicated to match the sequence length.

>>> from psyrun import Param
>>> pspace = Param(a=[1, 2, 3], b=[4, 5, 6], c=7)

The :class:`.Param` object only stores the information to construct all
parameter assignments. Call the :func:`.Param.build` method to construct
a dictionary with these parameter assignments. The dictionary will have the
parameter names as keys and lists of the assigned values in corresponding order
as values.

>>> from pprint import pprint
>>> pprint(pspace.build())
{'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 7, 7]}

So far it would have been easier to just enter the resulting dictionary in the
first place. But the :class:`.Param` class allows to easily construct more
complicated parameter spaces. If you multiply two :class:`.Param` instances
this will result in the Cartesian product of the parameter values.

>>> pspace = Param(a=[1, 2, 3], b=[4, 5, 6]) * Param(c=[7, 8])
>>> pprint(pspace.build())
{'a': [1, 1, 2, 2, 3, 3], 'b': [4, 4, 5, 5, 6, 6], 'c': [7, 8, 7, 8, 7, 8]}

It is also possible to concatenate parameter spaces with the summation operator.

>>> pspace = Param(a=[1, 2]) + Param(a=[2, 3], b=[4, 4])
>>> pprint(pspace.build())
{'a': [1, 2, 2, 3], 'b': [nan, nan, 4, 4]}

As you can see, missing values will be filled with ``nan``.

There are two ways to exclude elements from a parameters space. The subtraction
operator removes everything with matching parameters.

>>> pspace = Param(a=[1, 2, 3], b=[1, 2, 3]) - Param(a=[2])
>>> pprint(pspace.build())
{'a': [1, 3], 'b': [1, 3]}

This however would raise an exception if there are additional parameters in the
subtrahend.

>>> pspace = Param(a=[1, 2, 3]) - Param(a=[2], c=[4])
Traceback (most recent call last):
    ...
psyrun.pspace.AmbiguousOperationError: Key `c` not existent in minuend.

In this case, the `missing` function can be used to determine all parameter
assignments missing in the second parameter space.

>>> from psyrun.pspace import missing
>>> pspace = missing(Param(a=[1, 2, 3]), Param(a=[2], c=[4]))
>>> pprint(pspace.build())
{'a': [1, 3]}

With these basic operations it is possible to construct complicated
parameter spaces. For very large spaces you might not want to convert the whole
space into a dictionary at once. For this case exists the
:func:`.Param.iterate` method to iterate over the parameter assignments without
allocating more memory than necessary.

>>> pspace = Param(a=[1, 2]) * Param(b=[3, 4])
>>> for p in pspace.iterate():
...     pprint(p)
{'a': 1, 'b': 3}
{'a': 1, 'b': 4}
{'a': 2, 'b': 3}
{'a': 2, 'b': 4}


Evaluating functions on parameter spaces
----------------------------------------

Once the parameter space is constructed, one probably wants to evaluate
a function on it. For this the function needs to accept a set of parameters as
keyword arguments and it has to return it results as a dictionary. Here is
a simple example function:

>>> def basic_math(a, b):
...     return {'sum': a + b, 'product': a * b}

The :func:`.map_pspace` function allows to easily map such a function onto a
parameter space.

>>> from pprint import pprint
>>> from psyrun import map_pspace, pspace
>>> pspace = Param(a=[1, 2]) * Param(b=[3, 4])
>>> pprint(map_pspace(basic_math, pspace))
{'a': [1, 1, 2, 2],
 'b': [3, 4, 3, 4],
 'product': [3, 4, 6, 8],
 'sum': [4, 5, 5, 6]}

This will evaluate each set of parameters serially. If the evaluated function
itself is not parallelized it is probably more efficient to do the evaluation
for different sets of parameter values in parallel. If you have
`joblib <https://pythonhosted.org/joblib/>`_ installed and your function can be
pickled (e.g., it can be imported from a Python module), you can use
:func:`.map_pspace_parallel` to parallelize the evaluation of parameter sets.

>>> from psyrun import map_pspace_parallel
>>> from psyrun.utils.example import square
>>> pprint(map_pspace_parallel(square, Param(x=[1, 2, 3])))
{'x': [1, 2, 3], 'y': [1, 4, 9]}


Psyrun command line interface
-----------------------------

All Psyrun commands are invoked with ``psy <subcommand>``. The available
subcommands are described in the following. The ``psy`` command looks for task
definitions in the *psy-tasks* directory relative to its working directory, but
a different location can be provided to many subcommands with the ``--taskdir``
argument. To get help about the ``psy`` command or any subcommand use ``psy
--help`` and ``psy <subcommand> --help``.

run
^^^

``psy run [-h] [--taskdir TASKDIR] [task [task ...]]``

Without further arguments this executes all tasks that are not up-to-date. Each
subtask will be printed out prefixed either with ``.`` (if the task is
executed) or ``-`` if the task is skipped. This corresponds to the conventions
used by `doit <http://pydoit.org/>`_. It is possible to only execute a subset
of tasks by explicitly naming them as arguments to the ``run`` subcommand.

Furthermore, the ``-c`` or ``--continue`` argument is accepted to preserve and
add to existing results.

clean
^^^^^

``psy clean [-h] [--taskdir TASKDIR] [task [task ...]]``

Clean one or more tasks passed as arguments to the command. This means
all files generated for the task will be deleted.

list
^^^^

``psy list [-h] [--taskdir TASKDIR]``

List the name of all tasks.

merge
^^^^^

``psy merge [-h] directory merged``

Merges all output files in *directory* into a single file *merged*.

status
^^^^^^

``psy status [-h] [--taskdir TASKDIR] [-v] [task [task ...]]``

Prints the status of all tasks or the tasks given as arguments. Use the ``-v``
flag for a more verbose output including parameter assignments that have not
been evaluated yet.

test
^^^^

``psy test [-h] [--taskdir TASKDIR] [task [task ...]]``

Runs a single parameter assignment for each task or each task given as argument
to test that it successfully executes. It does not use the scheduler defined in
the task file to submit jobs, but will directly run them.


Writing task-files
------------------




Data stores
---------------------

Psyrun can use different “data stores” to persist data to the hard drive. It
provides three stores with different advantages and disadvantages described in
the following. It is possible to provide additional stores by implementing the
`AbstractStore` interface.

Note that Psyrun almost always needs to merge multiple data files and thus the
performance of appending can to an existing data file can be quite relevant.
The only store that supports efficient append is the `HDF5Store` at the moment.
If you have the possibility to use it, it should probably be your first choice.
The `NpzStore` should be the second choice. The default `PickleStore` is the
least efficient choice, but provides support for the widest range of data types
and has no additional dependencies.

pickle
^^^^^^

The `PickleStore` is the default because it has no additional dependencies and
supports all data types that can be pickled. It can be slow with large data
files and appending requires the complete file to be loaded and rewritten.

NumPy NPZ
^^^^^^^^^

The `NpzStore` requires `NumPy <http://www.numpy.org/>`_ and is more efficient
than the `PickleStore`. It will, however, also require to read and rewrite the
complete data file for appending data.

HDF5
^^^^

The `H5Store` requires `PyTables <http://www.pytables.org/>`_ and provides
efficient appends to existing data files. However, it only supports numeric
data types.


Backends
--------

Distribute
^^^^^^^^^^

LoadBalancing
^^^^^^^^^^^^^

Sqsub scheduler (Sharcnet)
--------------------------

Interfacing other schedulers
----------------------------
