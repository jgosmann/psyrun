Changes
=======

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
