:description: Creating custom data types for the sqlite databases.

Custom data types
=================

.. rst-class:: lead

    Create custom data types and use them in your databases.

----

Introduction
------------
sl3aio's :py:mod:`.dataparser` module uses sqlite3's adapters and converters system to create loaders and
dumpers for the data types. In this library object that contains load and dump methods for other objects is
:py:class:`.Parser`.

Listing allowed types and typenames
-----------------------------------
In some cases, you may need a list of allowed types or typenames. (For example, you want to check whether your
custom type is allowed to be used as a column type)

Allowed types
~~~~~~~~~~~~~
You can access the list of allowed types with :py:func:`.allowed_types`:

.. code-block:: python

    from sl3aio import allowed_types

    allowed_types = allowed_types()

.. Note::
    This method returns a **set** of types that can be written into the database.

Allowed typenames
~~~~~~~~~~~~~~~~~
You can access the list of allowed typenames with :py:func:`.allowed_typenames`:

.. code-block:: python

    from sl3aio import allowed_typenames

    allowed_typenames = allowed_typenames()

.. Note::
    This method returns a **set** of strings that can be column's type in the database.

    For default data types, this list includes **only their affinities**. So there are no
    such typenames as ``DOUBLE``, ``TINYINT``, ``VARCHAR(...)`` and etc. in the result set.

Custom parsers
--------------
You can define your own parsers for custom types using the :py:class:`.Parser` class. Let's create a parser for
the 2D point for example.

First import the :py:class:`.Parser` class:

.. code-block:: python

    from sl3aio import Parser

Then create a type for the 2D point:

.. code-block:: python

    class Point2D:
        def __init__(self, x: float, y: float) -> None:
            self.x = x
            self.y = y

Now create loads (*converts data of* :py:data:`.DefaultDataType`, *recieved from the table, to python object*)
and dumps (*converts python object to any of the allowed types, listed in* :py:func:`.allowed_types` *method*)
methods for this type:

.. code-block:: python

    def loads(data: str) -> Point2D:
        point = data.split()
        return float(point[0]), float(point[1])


    def dumps(point: Point2D) -> str:
        return f'{point.x} {point.y}'

.. Note::
    The type of data, recieved from the table by ``loads`` method must will be the same as the return type of
    the ``dumps`` method.
    
    If the ``dumps`` method returns an other type, that has its own parser, then the ``loads`` method
    will receive date the same type as the return type of this other type (and so on until the return type
    of ``dumps`` won't be one of the :py:data:`.DefaultDataType`).

Finally create and register the parser:

.. code-block:: python

    point_parser = Parser(
        types={Point2D},
        _typenames={'Point2D', '2dpoint'},
        loads=loads,
        dumps=dumps
    ).register()

.. Hint::
    :class: dropdown

    - The :py:class:`.Parser` constructor takes the following parameters:
        1. ``types``: Set of the types corresponding to the parser.
        2. ``_typenames``: Set of the typenames (column types) corresponding to the parser. Every given typename
           will be converted to uppercase during initialization.
        3. ``loads``: Method for converting data from the table to python object.
        4. ``dumps``: Method for converting python object to the type corresponding to the parser.
    - The :py:meth:`.Parser.register` method registrates loads and dumps methods as the sqlite3's converters
      and adapters.
    - Use the :py:meth:`.Parser.unregister` method to remove the converters and adapters from sqlite3.

Now you can use Point2D type in your database.

.. Tip::
    You can obtain the parser later by the desired type or typename using the following methods:

    .. code-block:: python

        # Using type
        point_parser = Parser.get_by_type(Point2D)

        # Using typename
        # (the given typename will be converted to uppercase automatically)
        point_parser = Parser.get_by_typename('Point2D')

Parsable objects
----------------
You can also create a parser from the :py:class:`.Parsable` subclasses instances that must implement the
:py:meth:`.Parsable.from_data` abstract classmethod that represents the ``loads`` and the
:py:meth:`.Parsable.to_data` abstract method that represents the ``dumps``.

Import the :py:class:`.Parser` and :py:class:`.Parsable` classes:

.. code-block:: python

    from sl3aio import Parsable, Parser

Create a type for the 2D point inherited from the :py:class:`.Parsable` and implement its abstract methods:

.. code-block:: python

    class Point2D(Parsable):
        def __init__(self, x: float, y: float) -> None:
            self.x = x
            self.y = y

        @classmethod
        def from_data(cls, data: str) -> 'Point2D':
            return cls(*map(float, data.split()))

        def to_data(self) -> str:
            return f'{self.x} {self.y}'

Now registrate the parser for the Point2D class using the :py:meth:`.Parser.from_parsable` classmethod:

.. code-block:: python

    point_parser = Parser.from_parsable(Point2D, typenames=['Point2D', '2dpoint']).register()

.. Hint::
    - The :py:meth:`.Parser.from_parsable` method takes the following parameters:
        1. ``parsable``: The subclass of the :py:class:`.Parsable` class.
        2. ``typenames``: An any iterable of strings that represent the typenames for this parsable, optional,
           defaults to the empty tuple. If not provided or empty, the uppercase name of the ``parsable``
           class is used.

Now you can use Point2D type in your database.

Built-in parsers
----------------
You can find several ready-made parsers in :py:class:`BuiltinParsers`. Some of them are are available
only after initialization.

.. Attention::
    Before using :py:attr:`.BuiltionParsers.BOOL`, :py:attr:`.BuiltionParsers.SET`,
    :py:attr:`.BuiltionParsers.TUPLE`, :py:attr:`.BuiltionParsers.JSON`, :py:attr:`.BuiltionParsers.TIME`,
    :py:attr:`.BuiltionParsers.DATE` and :py:attr:`.BuiltionParsers.DATETIME` parsers, you must call
    :py:meth:`.BuiltinParsers.init` method that creates and registrates all these parsers.

.. Warning::
    Do not registrate :py:attr:`.BuiltionParsers.BLOB`, :py:attr:`.BuiltionParsers.INT`,
    :py:attr:`.BuiltionParsers.REAL` and :py:attr:`.BuiltionParsers.TEXT` parsers using their's
    :py:meth:`.Parser.register` method.

- :py:attr:`.BuiltinParsers.BLOB`: Parser for ``bytes`` objects and ``BLOB``, ``BYTES`` columns.
- :py:attr:`.BuiltinParsers.INT`: Parser for ``int`` objects and ``INT``, ``INTEGER`` columns.
- :py:attr:`.BuiltinParsers.REAL`: Parser for ``float`` objects and ``REAL``, ``FLOAT``, ``DOUBLE`` columns.
- :py:attr:`.BuiltinParsers.TEXT`: Parser for ``text`` objects and ``TEXT``, ``CHAR``, ``VARCHAR``, ``STR``,
  ``STRING`` columns.
- :py:attr:`.BuiltinParsers.BOOL`: Parser for ``bool`` objects and ``BOOLEAN``, ``BOOL`` columns.
- :py:attr:`.BuiltinParsers.JSON`: Parser for ``list``, ``dict`` objects and ``LIST``, ``DICT``, ``JSON``
  columns.
- :py:attr:`.BuiltinParsers.TUPLE`: Parser for ``tuple`` object and ``TUPLE`` columns.
- :py:attr:`.BuiltinParsers.SET`: Parser for ``set`` object and ``SET`` columns.
- :py:attr:`.BuiltinParsers.TIME`: Parser for ``datetime.time`` objects and ``TIME`` columns in one of the
  `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ formats.
- :py:attr:`.BuiltinParsers.DATE`: Parser for ``datetime.date`` objects and ``DATE`` columns in the
  `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.
- :py:attr:`.BuiltinParsers.DATETIME`: Parser for ``datetime.datetime`` objects and ``DATETIME`` columns in the
  `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.

So, if you want to use boolean values in your database, you can call the :py:meth:`.BuiltinParsers.init` method
before accessing the tables, and then simply use them.

.. code-block:: python

    from sl3aio import BuiltinParsers

    BuiltinParsers.init()
    await table.insert(some_bool_value=True)

If you want to change the load/dump method of a parser after initialization, you can do so:

.. code-block:: python

    from sl3aio import BuiltinParsers


    def new_date_dumps(obj):
        # Your new dumps logic here
    

    def new_date_loads(data):
        # Your new loads logic here


    BuiltinParsers.DATE.dumps = new_date_dumps

    BuiltinParsers.init()
    BuiltinParsers.DATE.dumps = new_date_dumps
    BuiltinParsers.DATE.loads = new_date_loads
    BuiltinParsers.DATE.register()  # Reregister the dumps/loads methods.
