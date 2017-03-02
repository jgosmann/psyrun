API Documentation
=================

Core interface
--------------

These modules constitute the core API of Psyrun and most likely to be used in
other programs.

backend module
^^^^^^^^^^^^^^

.. automodule:: psyrun.backend

backend.base
""""""""""""

.. automodule:: psyrun.backend.base

backend.distribute
""""""""""""""""""

.. automodule:: psyrun.backend.distribute

backend.load_balancing
""""""""""""""""""""""

.. automodule:: psyrun.backend.load_balancing

exceptions
^^^^^^^^^^

.. automodule:: psyrun.exceptions

mapper module
^^^^^^^^^^^^^

.. automodule:: psyrun.mapper

pspace module
^^^^^^^^^^^^^

.. automodule:: psyrun.pspace

store module
^^^^^^^^^^^^

.. automodule:: psyrun.store

store.base
""""""""""

.. automodule:: psyrun.store.base

store.h5
""""""""

.. automodule:: psyrun.store.h5

store.npz
"""""""""

.. automodule:: psyrun.store.npz

store.pickle
""""""""""""

.. automodule:: psyrun.store.pickle

scheduler module
^^^^^^^^^^^^^^^^

.. automodule:: psyrun.scheduler

tasks module
^^^^^^^^^^^^

.. automodule:: psyrun.tasks


Supporting interfaces
---------------------

These modules provide functionality that is still central to Psyrun, but are
usually not required to be accessed in other programs.

main
^^^^

.. automodule:: psyrun.main

jobs
^^^^

.. automodule:: psyrun.jobs


Utilities
---------

The utility modules provide various things that are not related to Psyrun's
core functionality. The API of utilities is not guaranteed to be stable across
versions.

.. automodule:: psyrun.utils

utils.doc module
^^^^^^^^^^^^^^^^

.. automodule:: psyrun.utils.doc

utils.example module
^^^^^^^^^^^^^^^^^^^^

.. automodule:: psyrun.utils.example

utils.testing module
^^^^^^^^^^^^^^^^^^^^

.. automodule:: psyrun.utils.testing

utils.venv module
^^^^^^^^^^^^^^^^^

.. automodule:: psyrun.utils.venv
