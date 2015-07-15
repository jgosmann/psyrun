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
this will result in the cross product of the parameter values.

>>> pspace = Param(a=[1, 2, 3], b=[4, 5, 6]) * Param(c=[7, 8])
>>> pprint(pspace.build())
{'a': [1, 1, 2, 2, 3, 3], 'b': [4, 4, 5, 5, 6, 6], 'c': [7, 8, 7, 8, 7, 8]}

It is also possible to concatenate parameter spaces with the summation operator.

>>> pspace = Param(a=[1, 2]) + Param(a=[2, 3], b=[4, 4])
>>> pprint(pspace.build())
{'a': [1, 2, 2, 3], 'b': [nan, nan, 4, 4]}

As you can see, missing values will be filled with ``nan``.

Finally, there is the subtraction operator to exclude elements from the
parameter space.

>>> pspace = Param(a=[1, 2, 3], b=[1, 2, 3]) - Param(a=[2])
>>> pprint(pspace.build())
{'a': [1, 3], 'b': [1, 3]}

With these three basic operations it is possible to construct complicated
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
