Getting started
===============


Requirements
------------

* `Python <https://www.python.org/>`_ >=2.7, >=3.4
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


Installation
------------

The easiest way to install Psyrun is with `pip
<https://pip.pypa.io/en/stable/>`_::

    pip install psyrun

All required dependencies should be installed automatically. If you want to use
features that require the optional NumPy dependency, it is best to install
NumPy first as the installation sometimes fails otherwise.

To be able to use the NPZ store::

    pip install numpy
    pip install 'psyrun[npz]'

To be able to use the HDF5 store::

    pip install numpy
    pip install 'psyrun[h5]'


Development install
^^^^^^^^^^^^^^^^^^^

To make a development install from the most recent Git checkout::

    git clone https://github.com/jgosmann/psyrun.git
    cd psyrun
    pip install -e .


Usage
-----

You can access the Psyrun API in Python programs with::

    import psyrun

The command line interface is available with the ``psy`` command. Type ``psy
--help`` or ``psy <command> --help`` to display help on the command line.


Running unit tests
------------------

Make sure all test requirements are installed with::

    cd psyrun
    pip install -r requirements-tests.txt

Then the tests can be run with the ``py.test`` command or the ``tox`` command to
test multiple Python versions.


Building the documentation
--------------------------

Make sure all required packages for the documentation are installed with::

    cd psyrun
    pip install -r requirements-docs.txt

To build the documentation::

    cd psyrun/doc
    make html

Open ``psyrun/doc/build/html/index.html`` to view the documentation.
