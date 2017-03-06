.. _guide:

User Guide
==========

.. _guide-pspace:

Constructing parameter spaces
-----------------------------

Parameter spaces are constructed with the `Param` class. You pass in
sequences of parameter values as keyword arguments. As long as you pass in at
least one sequence, other parameter values are allowed to be scalars and will be
replicated to match the sequence length.

>>> from psyrun import Param
>>> pspace = Param(a=[1, 2, 3], b=[4, 5, 6], c=7)

The `Param` object only stores the information to construct all
parameter assignments. Call the :meth:`~.ParameterSpace.build` method to construct
a dictionary with these parameter assignments. The dictionary will have the
parameter names as keys and lists of the assigned values in corresponding order
as values.

>>> from pprint import pprint
>>> pprint(pspace.build())
{'a': [1, 2, 3], 'b': [4, 5, 6], 'c': [7, 7, 7]}

So far it would have been easier to just enter the resulting dictionary in the
first place. But the `Param` class allows to easily construct more
complicated parameter spaces. If you multiply two `Param` instances,
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

>>> pspace = Param(a=[1, 2, 3]) - Param(a=[2], c=[4])  # doctest: +IGNORE_EXCEPTION_DETAIL
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
space into a dictionary at once. For this case the
:func:`.Param.iterate` method exists to iterate over the parameter assignments without
allocating more memory than necessary.

>>> pspace = Param(a=[1, 2]) * Param(b=[3, 4])
>>> for p in pspace.iterate():
...     pprint(p)
{'a': 1, 'b': 3}
{'a': 1, 'b': 4}
{'a': 2, 'b': 3}
{'a': 2, 'b': 4}


.. _guide-mapping:

Evaluating functions on parameter spaces
----------------------------------------

Once the parameter space is constructed, one probably wants to evaluate
a function on it. For this, the function needs to accept a set of parameters as
keyword arguments and it has to return its results as a dictionary. Here is
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
itself is not parallelized, it is probably more efficient to do the evaluation
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
a different location can be provided with the ``--taskdir``
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

Merges all output files in *directory* into a single file *merged*. The filename
extension of *merged* is used to determine the input and output format.

status
^^^^^^

``psy status [-h] [--taskdir TASKDIR] [-v] [task [task ...]]``

Prints the status of all tasks or the tasks given as arguments. Use the ``-v``
flag for a more verbose output including parameter assignments that have not
been evaluated yet.


.. _cmd-test:

test
^^^^

``psy test [-h] [--taskdir TASKDIR] [task [task ...]]``

Runs a single parameter assignment for each task or each task given as argument
to test that it successfully executes. It does not use the scheduler defined in
the task file to submit jobs, but will directly run them.


.. _guide-task-files:

Writing task-files
------------------

Each task is defined in a Python file with the name ``task_<name>.py``. That
means any valid Python code can be used in the definition of the task. There
are certain module level variables that have a special meaning. The two most
important are ``pspace``, defining the parameter space to explore, and
``execute`` defining the function to evaluate a single parameter assignment.

Also consider setting ``store`` to either `H5Store` or `NpzStore`. This
requires additional dependencies to be installed and imposes some limitations
on the data, but can improve performance. See :ref:`guide-stores` for more
details.

It is likely that you also want to adjust *max_jobs* (maximum number of
processing jobs to submit to process the task) and *min_items* (minimum
number of items to process with each processing jobs). If each parameter
assignment is evaluated quickly, it can be beneficial to increase *min_items*
to avoid the overhead of starting a lot of jobs. By default *max_jobs* is set
to 100 as on high performance clusters there might be a penalty or limit on the
number of jobs one can submit at a time.

If you want to run a task on a high performance cluster, it will be necessary
to set *scheduler* to the appropriate scheduler. Otherwise, jobs will be run
serially and immediately. There is also a *schedular_args* variable which
allows to define a dictionary of additional required arguments for the
scheduler. These will depend on the scheduler used, see :ref:`guide-schedulers`
for more details.
High performance clusters might offer different file systems with different
access speed. In that case you might want to set *workdir*, the directory
where intermediary files are written to, and *resultfile*, the file results
are written to, to appropriate locations.

By default Psyrun will split the parameters space in equally sized batches. If
parameter assignment can vary in their execution time, it might be beneficial
to use a load balancing approach by setting *backend* to
`LoadBalancingBackend`. See :ref:`guide-backends` for more details.

All special variables are documented as part of the `psyrun.tasks.Config`
documentation.

This is what a task file to run on the `Sharcnet <https://www.sharcnet.ca>`_
might look like::

    import numpy as np
    from psyrun import Param, Sqsub
    from psyrun.store.npz import NpzStore


    pspace = Param(radius=np.linspace(0., 1., 100)) * Param(trial=np.arange(50))
    min_items = 10
    store = NpzStore()
    workdir = '/work/user/mc_circle_area'
    scheduler = Sqsub(workdir)
    scheduler_args = {
        'timelimit': '15m',
        'memory': '1G',
    }


    def execute(radius, trial):
        n = 100
        x = np.random.random((n, 2)) * 2. - 1.
        return {'a_frac': np.mean(np.linalg.norm(x, axis=1) < radius), 'x': x}


.. _guide-stores:

Data stores
-----------

Psyrun can use different “data stores” to persist data to the hard drive. It
provides three stores with different advantages and disadvantages described in
the following. It is possible to provide additional stores by implementing the
`Store` interface.

Note that Psyrun almost always needs to merge multiple data files and thus the
performance of appending to an existing data file can be quite relevant.
The only store that supports efficient appending is the `H5Store` at the moment.
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
than the `PickleStore`. It will, however, still require to read and rewrite the
complete data file for appending data.

HDF5
^^^^

The `H5Store` requires `PyTables <http://www.pytables.org/>`_ and provides
efficient appends to existing data files. However, it only supports numeric
data types.


.. _guide-backends:

Backends
--------

Backends determine how work is distributed to a number of jobs. By default
Psyrun will use the `DistributeBackend` that will use one job to split the
parameter space in equally sized batches and process them with up to
*max_jobs* processing jobs (each batch will have at least *min_items* items
to process). After all processing jobs are finished all the results will be
merged into a single file by another job. This is similar to `map-reduce
processing <https://de.wikipedia.org/wiki/MapReduce>`_.

If evaluating different parameter sets can take a different amount of time,
this might lead to some jobs finishing very early, while others take a long
time. Thus the computational resources are not used optimally. In that case in
can be beneficial to use load balancing with the `LoadBalancingBackend`. This
backend will start *max_jobs* and each will fetch single items to process until
all items have been processed. Thus, if a job is finished early with one item,
it just fetches the next and continues. This gives a better use of the
computational resources, but also has some disadvantages: It requires to load
specific single rows from an input file which is only supported efficiently by
the `H5Store`. Also the order in which the results are written becomes
non-deterministic which makes it computationally more expensive to determine
what parameter assignments have to be rerun if some of them failed to execute.


.. _guide-schedulers:

Schedulers
----------

Schedulers define how Psyrun submits individual jobs. The default is
`ImmediateRun` which is not really a scheduler because it just immediately runs
any job on submission. Psyrun comes with support for Sharcnet's ``sqsub`` with
the `Sqsub` scheduler. For other schedulers it is necessary
to write some custom code.

Sqsub scheduler (Sharcnet)
^^^^^^^^^^^^^^^^^^^^^^^^^^

The `Sqsub` scheduler uses ``sqsub`` to submit jobs. It accepts the following
*scheduler_args* (corresponding ``sqsub`` command line options are given in
parenthesis):

* *timelimit* (required, ``-r``): String stating the execution time limit for
  each individual job.
* *n_cpus* (optional, default 1, ``-n``): Number of CPU cores to allocate for
  each individual job.
* *n_nodes* (optional, ``-N``): Number of nodes to allocate for each individual
  job.
* *memory* (required, ``--mpp``): String stating the memory limit for each
  individual job.

For more details see the ``sqsub`` help.


Interfacing other schedulers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To support other schedulers, it is necessary to implement the `Scheduler`
interface. The central function is `Scheduler.submit` that will be invoked to
submit a job. Furthermore, functions to obtain the status
(`Scheduler.get_status`), return running and queued jobs
(`Scheduler.get_jobs`), and kill jobs `Scheduler.kill` are required. It can be
instructive to read the `Sqsub` source code before
implementing a scheduler.


Recipes
-------

This section collects code examples for common tasks.


Convert results to a Pandas data frame
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Note that this recipe requires all single parameter values and outputs to be
scalars as Pandas does not support multi-dimensional data.

.. code-block:: python

    import pandas as pd
    import psyrun

    store = psyrun.store.PickleStore()  # insert appropriate store here
    df = pd.DataFrame(store.load('path/to/datafile.pkl'))
