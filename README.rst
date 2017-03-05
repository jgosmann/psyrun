Psyrun
======

.. image:: https://travis-ci.org/jgosmann/psyrun.svg?branch=master
    :target: https://travis-ci.org/jgosmann/psyrun

.. image:: https://coveralls.io/repos/github/jgosmann/psyrun/badge.svg?branch=master
    :target: https://coveralls.io/github/jgosmann/psyrun?branch=master

Psyrun is a Python_ tool to define parameter spaces
and execute an evaluation function for each parameter assignment. In addition
Psyrun makes it easy to use serial farming, i.e. evaluating multiple parameter
assignments in parallel, on a multicore computers and high-performance clusters.

Documentation
-------------

`The documentation can be found here.
<http://psyrun.readthedocs.io/en/latest/>`_

Overview
--------

Define parameter spaces and evaluate them:

.. code-block:: python

    from psyrun import map_pspace, Param

    def objective(a, b, c):
        return a * b + c

    pspace = (Param(a=np.arange(1, 5))
              * Param(b=np.linspace(0, 1, 10))
              * Param(c=[1., 1.5, 10., 10.5]))
    results = map_pspace(objective, pspace) 

Or do it in parallel:

.. code-block:: python

    from psyrun import map_pspace_parallel
    results = map_pspace_parallel(objective, pspace)

Define tasks by placing ``task_<name>.py`` files in the `psy-tasks`` directory:

.. code-block:: python

    from psyrun import Param

    pspace = (Param(a=np.arange(1, 5))
              * Param(b=np.linspace(0, 1, 10))
              * Param(c=[1., 1.5, 10., 10.5]))

    def execute(a, b, c):
        return {'result': a * b + c}

and run them by typing ``psy run`` with support for serial farming on high
performance clusters.


Installation
------------

``pip install psyrun``

To be able to use the NPZ store::

    pip install numpy
    pip install 'psyrun[npz]'

To be able to use the HDF5 store::

    pip install numpy
    pip install 'psyrun[h5]'


Requirements
------------

* Python_ >=2.7, >=3.4
* `six <https://pypi.python.org/pypi/six>`_

Optional requirements
^^^^^^^^^^^^^^^^^^^^^

To have `faulthandler <http://faulthandler.readthedocs.io/>`_ activated for
jobs submitted with ``psy run`` in Python 2.7:

* `faulthandler <http://faulthandler.readthedocs.io/>`_

Python 3.4+ already includes the faulthandler module.

To use ``map_pspace_parallel``:

* `joblib <https://pythonhosted.org/joblib/>`_

To use NPZ files as store:

* `NumPy <http://www.numpy.org/>`_

To use HDF5 files as store:

* `NumPy <http://www.numpy.org/>`_
* `pytables <http://www.pytables.org/>`_

To run the unit tests:

* `joblib <https://pythonhosted.org/joblib/>`_
* `NumPy <http://www.numpy.org/>`_
* `pytables <http://www.pytables.org/>`_
* `pytest <http://doc.pytest.org/en/latest/>`_ >= 2.8


To build the documentation:

* `numpydoc <https://pypi.python.org/pypi/numpydoc>`_


.. _Python: https://www.python.org/
