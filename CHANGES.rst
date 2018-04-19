Changes
=======


0.7.1 (unreleased)
------------------

Documentation improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^

* Added documentation for the ``pool_size`` task attribute and included it in
  the task template.

Bug fixes
^^^^^^^^^

* Allow to continue tasks if results are missing even if the result file is
  newer than the task file.


0.7.0 (February 18, 2018)
-------------------------

New features
^^^^^^^^^^^^

* Added support for the Slurm Workload Manager.


0.6.0
-----

New features
^^^^^^^^^^^^

* Add ``psy new-task`` and ``psy kill`` commands.
* Added ``AutodetectStore`` that determines the appropriate store from the
  filename extension.
* Added possibility to let ``psy merge`` custom stores if provided as
  ``psyrun.stores`` entry point.
* Added capability to set scheduler arguments based on the job name.


0.5.4
-----

Bug fixes
^^^^^^^^^

* Fix the ``psy run`` continue functionality.


0.5.3
-----

Bug fixes
^^^^^^^^^

* Fix ``psy status`` and
  ``psyrun.backend.distribute.DistributeBackend.get_missing`` trying to read
  incompatible data files in the output directory.
* Fix ``psy status`` and
  ``psyrun.backend.distribute.DistributeBackend.get_missing`` easily hitting
  Python's recursion depth limit.
* Fix merging of npz files with missing integer values by converting them to
  float where ``np.nan`` can be used.


0.5.2
-----

Bug fixes
^^^^^^^^^

* Fix incorrect ``psy status``.
* Fix ``psy run <task1> <task2> ...`` not running all tasks and run them in
  order.


0.5.1
-----

Bug fixes
^^^^^^^^^

* Fix ``psy merge`` always assuming ``PickleStore``.

Documentation improvements
^^^^^^^^^^^^^^^^^^^^^^^^^^

* Add recipe for converting data to Pandas data frame to documentation.


0.5
---

* Initial release
