"""
sl3aio
======

A simple and efficient Pytohn library for asynchronous use of SQLite3 with a set of useful tools and interfaces.

Modules
-------
- :mod:`sl3aio.easytable`: Provides a simple and efficient library for working with tables.
- :mod:`sl3aio.dataparser`: Provides utility functions for parsing and manipulating data in various formats.
- :mod:`sl3aio.table`: Provides classes for interacting with SQLite tables.
- :mod:`sl3aio.executor`: Provides asynchronous wrappers and utilities for working with SQLite databases and
  event loop.

.. Note::
    You can import every sl3aio's component directly: ``from sl3aio import EasyTable, TableColumn, ...``.

Links
-----
- `Documentation <https://sl3aio.readthedocs.io>`_
- `GitHub <https://github.com/Nedelis/sl3aio/>`_
- `BSD 3-Clause License <https://github.com/Nedelis/sl3aio/blob/master/LICENSE>`_
"""
__version__ = '1.3.0-rc1'

from .table import *
from .executor import *
from .dataparser import *
from .easytable import *
