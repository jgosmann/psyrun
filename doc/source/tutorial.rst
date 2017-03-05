Tutorial
========

This tutorial will walk you through the main features of Psyrun. Depending on
your usage you might want to read up on more details in the detailed
:ref:`guide`.

In this tutorial it is assumed that `NumPy <http://www.numpy.org/>`_ has been
imported with::

    import numpy as np

But it is not a strict requirement to use Psyrun.


Parameter space exploration
---------------------------

Assume we have a function *objective* that we want to evaluate for different
parameters::

    def objective(a, b, c):
        return {'result': a * b + c}

In standard Python this would require to nest a bunch of for-loops like so::

    results = []
    for a in np.arange(1, 5):
        for b in np.linspace(0, 1, 10):
            for c in [1., 1.5, 10., 10.5]:
                results.append(objective(a, b, c)['result'])

For a complex function with a lot of parameters, this can get a quite deep
nesting! Psyrun allows you to do this more conveniently by defining a parameter
space with the `Param` class::

    from psyrun import map_pspace, Param

    pspace = (Param(a=np.arange(1, 5))
              * Param(b=np.linspace(0, 1, 10))
              * Param(c=[1., 1.5, 10., 10.5]))
    results = map_pspace(objective, pspace)

The multiplication operator ``*`` is defined as the Cartesian product on
`Param` instances. Similar code to the example above could also be written
with the Python *itertools* module. But the `Param` class provides a number
of other useful operations like concatenation or difference operations explained
in more detail in :ref:`guide-pspace`. It is also the basis to the usage of the
parallelization and serial farming features.


Parallelization
---------------

It is easy to evaluate multiple parameter assignments in parallel with Psyrun::

    from psyrun import map_pspace_parallel

    results = map_pspace_parallel(objective, pspace)

This parallelization is based on `joblib <https://pythonhosted.org/joblib/>`_
which by default uses the *multiprocessing* module that spawns multiple Python
processes. This requires, however, that the *objective* function can be
imported from a module, i.e. this does not work if it is only defined in an
interactive interpreter session. More details are to be found in the
:ref:`guide-mapping` section.


Tasks
-----

Tasks are actually the main feature of Psyrun. To see what makes them useful,
it is easiest to define a task and then see what we can do with it. The Psyrun
``psy`` command looks
for tasks in the *psy-tasks* directory relative to the current directory by
default. Each tasks is defined in a Python file named ``task_<name>.py``. For
example, we could define a task *example* with a few lines in
a file ``psy-tasks/task_example.py``::

    import numpy as np
    from psyrun import Param

    pspace = (Param(a=np.arange(1, 5))
              * Param(b=np.linspace(0, 1, 10))
              * Param(c=[1., 1.5, 10., 10.5]))

    def execute(a, b, c):
        return {'result': a * b + c}

Note that ``pspace`` and ``execute`` are names with a special meaning in this
task file. The ``pspace`` variable defines the parameter space explored in the
task and ``execute`` is the function to be invoked with each parameter
assignment. It has to return a dictionary which allows to return multiple,
named values.

We can now run this task by invoking ``psy run example`` on the command line
(or just ``psy run`` to run all defined tasks and not just *example*). This
will create a directory *psy-work/example* with a bunch of files supporting
the task execution and most importantly the file
``psy-work/example/result.pkl``, a Python
`pickle file <https://docs.python.org/3.6/library/pickle.html>`_ with the
results::

    import pickle

    with open('psy-work/example/result.pkl', 'rb') as f:
        print(pickle.load(f))
        # prints:
        # {'b': [0.66666666666666663, 0.44444444444444442, ...],
        #  'a': [1, 2, 2, 2, 2, 4, 4, 1, 1, 2, 2, 2, 2, 3, 3, 1, 2, 2, ...],
        #  'c': [1.5, 1.0, 1.5, 1.0, 1.5, 1.0, 1.5, 10.5, 1.0, 1.0, 1.5, ...],
        #  'result': [2.1666666666666665, 1.8888888888888888, ...]}

If you execute ``psy run`` again it will automatically detect whether the
results are still up-to-date and only rerun the tasks if it needs to be
updated.

One advantage of using the ``psy run`` command is that partial results will be
written to the disks in *psy-work/example/out*. This means if the certain
parameter assignments fail with an exception, not everything is lost. The
individual files in the *out* directory can be merged into a result file with
``psy merge psy-work/example/out partial-result.pkl``. To get information on
which results are missing use the the ``psy status -v example`` command

Sometimes it is desirable to add the results of additional parameters
assignments to the existing result. This can be done by editing the task file
and then using ``psy run --continue example`` to instruct Psyrun to preserve
the existing results and add new parameter assignments.

Psyrun uses pickle files by default because they support the most data types.
Unfortunately they are not the most efficient. Psyrun allows to use
`NumPy <http://www.numpy.org/>`_ NPZ files or
`HDF5 <https://support.hdfgroup.org/HDF5/>`_ instead. See :ref:`guide-stores`
for details.


Serial farming
--------------

If you have access to a high performance computing (HPC) cluster, you can use
Psyrun for serial farming. That means you run a large number of serial jobs,
i.e. jobs that have no interdependency and can be run in any order, on the
cluster. To do so you have to set the *scheduler* and *scheduler_args*
variables in your task file to the appropriate value (it also a good idea to
set *max_jobs* and *min_items*). More details can be found in
:ref:`guide-task-files`.

Psyrun comes with support for `Sharcnet <https://www.sharcnet.ca>`_'s
`sqsub <https://www.sharcnet.ca/help/index.php/Sqsub>`_ scheduler. If your
HPC cluster uses a different scheduler, you will have to write some code to
inform Psyrun on how to interface the scheduler.

It can be useful to test a task first by running a single parameter assignment
with the :ref:`cmd-test` command.
