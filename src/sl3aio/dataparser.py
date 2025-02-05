"""
sl3aio.dataparser
=================

This module provides a flexible and extensible system for parsing and converting data 
between Python objects and SQLite database representations. It offers tools for 
creating custom parsers, handling various data types, and managing the conversion 
process for database operations.

Key Components:
---------------
1. :class:`DefaultDataType`: Type alias for basic types natively supported by SQLite.
2. :class:`Parsable`: Abstract base class for creating custom parsable objects.
3. :class:`Parser`: Class for creating and managing custom data parsers.
4. :class:`BuiltinParsers`: Container for default and additional pre-defined parsers.

Features:
---------
- Support for native SQLite data types and custom Python objects
- Extensible parser system for handling complex data types
- Automatic registration and management of SQLite adapters and converters
- Built-in parsers for common Python types (e.g., bool, set, list, dict, datetime)
- Utility functions for querying allowed types and type names

Usage:
------
This module is designed to be used in conjunction with the sl3aio library for SQLite 
database operations. It provides the necessary tools to seamlessly convert between 
Python objects and their SQLite representations.

.. important::
    If you create custom parsers, you should always set the connection's parameter
    ``detect_types`` to ``sqlite3.PARSE_DECLTYPES``.

Examples:
---------
- If you need to work with booleans, or date and time, or lists, tuples, sets and dicts,
  or json objects, you don't need to create custom parsers for them. Call
  :func:`BuiltinParsers.init()` method and you will be able to work with these types.

.. code-block:: python

    from sl3aio import BuiltinParsers

    BuiltinParsers.init()

- If you need to work with a custom data type, you can create a custom parser for it.
  Here is an example.

.. code-block:: python

    from sl3aio import Parser
    from dataclasses import dataclass

    @dataclass
    class Point:
        x: float
        y: float

    # Here we defining the parser for the point's class.
    # 'types' is a set of types associated with this parser.
    # '_typenames' is a set of typenames (column types in sqlite) associated with this parser.
    # 'loads' is a function that converts bytes to the Point.
    # 'dumps' is a function that converts the Point to bytes or other parsable object.
    point_parser = Parser(
        types={Point},
        _typenames={'POINT'},
        loads=lambda data: Point(*map(float, data.decode('ascii').split())),
        dumps=lambda obj: f'{obj.x} {obj.y}'.encode('ascii')
    ).register()
    # Note that the 'register()' method must be called in order for sqlite to know about the parser.

- This example demonstrates how to create a custom parsable object and register it 
  with the parser system.

.. code-block:: python

    from sl3aio import Parser, Parsable
   
    
    class CustomObject(Parsable):
        def __init__(self, value):
            self.value = value
        
        @classmethod
        def from_data(cls, data: bytes):
            return cls(int.from_bytes(data, 'big'))
        
        def to_data(self):
            return self.value.to_bytes(4, 'big')

    
    custom_parser = Parser.from_parsable(CustomObject, ['CUSTOM'])
    custom_parser.register()
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from json import loads, dumps
from datetime import datetime, date, time
from collections.abc import Callable, Iterable
from typing import TypeAlias, ClassVar, Self, final
from sqlite3 import adapters, converters, PrepareProtocol

__all__ = ['DefaultDataType', 'allowed_types', 'allowed_typenames', 'Parser', 'Parsable', 'BuiltinParsers']

DefaultDataType: TypeAlias = bytes | str | int | float | None
"""Types that are supported by sqlite3 natively."""


def allowed_types() -> set[type]:
    """List types that can be written into database.
    
    Returns
    -------
    `set` [`type`]: Set of writable types.
    """
    return {bytes, str, int, float, None, *(k[0] for k in adapters)}


def allowed_typenames() -> set[str]:
    """List names that can be used as column type in database.
    
    Returns
    -------
    `set` [`str`]: Set of allowed columns types.

    .. note::
        For default data types, this list includes only their affinities. So there are no
        such typenames as ``DOUBLE``, ``TINYINT``, ``VARCHAR(...)`` and etc. in the result set.
    """
    return {'BLOB', 'TEXT', 'INTEGER', 'REAL', 'NUMERIC', *converters}


class Parsable(ABC):
    """Base class for custom parsable objects.
    
    See Also
    --------
    :class:`Parser`: Class for creating custom parsers.
    """
    @classmethod
    @abstractmethod
    def from_data(cls, data: bytes) -> Self:
        """Create an instance from a binary data.
        
        Parameters
        ----------
        data : `bytes`
            Incoming data from the sqlite database.

        Returns
        -------
        :class:`Parsable`: Instance of the parsable class.
        """

    @abstractmethod
    def to_data(self) -> 'Parsable | DefaultDataType':
        """Converts self to the any object of the allowed type, listed in :func:`allowed_types()`.
        
        Returns
        -------
        :class:`Parsable` | :obj:`DefaultDataType`: Object that can be written into sqlite database.
        """


@dataclass(slots=True)
class Parser[T]:
    """Class for creating custom parsers.
    
    Automates the registration of converters and adapters in sqlite3. Provides
    convinient access to the already registered parsers.

    Attributes
    ----------
    instances : `set` [:class:`Parsable`]
        Container for all of the parsers that were created.
    types : `set` [`type` [`T`]]
        Set of types corresponding to the parser. Must be at least one type in this set!
    typenames : `set` [`str`]
        Set of names corresponding to the parser. Must be at least one type name in this set!
    loads : `Callable` [[`bytes`], `T`]
        Method to parse a data from bytes.
    dumps : `Callable` [[`T`], :class:`Parsable` | :class:`DefaultDataType`]
        Method to convert an object to the object of the allowed type, listed in :func:`allowed_types()`.

    See Also
    --------
    :class:`Parsable`
        Base class for custom parsable objects.
    :class:`BuiltinParsers`
        Useful builtin data parsers.
    """
    instances: ClassVar[set[Self]] = set()
    types: set[type[T]]
    _typenames: set[str]
    loads: Callable[[bytes], T] = field(repr=False)
    dumps: Callable[[T], Parsable | DefaultDataType] = field(repr=False)

    @property
    def typenames(self) -> set[str]:
        return self._typenames
    
    @typenames.setter
    def typenames(self, typenames: Iterable[str]) -> None:
        self._typenames = set(map(str.upper, typenames))

    @classmethod
    def from_parsable(cls, parsable: type[Parsable], typenames: Iterable[str] = ()) -> Self:
        """Construct a new instance from a parsable object and optional typenames.
        
        Parameters
        ----------
        parsable : `type` [:class:`Parsable`]
            Object that can be loaded and dumped using its own converters.
        typenames : `Iterable` [`str`], optional
            Optional typenames. If not provided name of the class will be used instead.
            Defaults to empty tuple.

        Returns
        -------
        :class:`Parser`: New instance of the parser.
        """
        return cls(
            {parsable},
            set(typenames) or {parsable.__name__},
            parsable.from_data,
            parsable.to_data
        )

    @classmethod
    def get_by_type(cls, _type: T) -> Self | None:
        """Get an instance of a parser from its registry by the type it supports.
        
        Parameters
        ----------
        _type : `T`
            Type that must be supported by required parser.

        Returns
        -------
        :class:`Parser` | `None`: Instance of the parser or None if no parser corresponding to the given
        type was found.
        """
        return next((parser for parser in cls.instances if _type in parser.types), None)
    
    @classmethod
    def get_by_typename(cls, _typename: str) -> Self | None:
        """Get an instance of a parser from its registry by the typename of type which it supports.
        
        Parameters
        ----------
        _typename : `str`
            Name of the type that must be supported by required parser.
        
        Returns
        -------
        :class:`Parser` | `None`: Instance of the parser or None if no parser corresponding
        to the given typename was found.
        """
        _typename = _typename.upper()
        return next((parser for parser in cls.instances if _typename in parser.typenames), None)

    def __post_init__(self) -> None:
        assert self.types, 'Parser must have at least one type corresponding to it!'
        assert self._typenames, 'Parser must have at least one typename corresponding to it!'
        self._typenames = set(map(str.upper, self._typenames))
        self.instances.add(self)

    def register(self) -> Self:
        """Register converters and adapters in sqlite3.
        
        Returns
        -------
        :class:`Parser`: Self for chaining.
        """
        for __typename in self.typenames:
            converters[__typename] = self.loads
        for __type in self.types:
            adapters[(__type, PrepareProtocol)] = self.dumps
        return self

    def unregister(self) -> Self:
        """Unregister converters and adapters in sqlite3.
        
        Returns
        -------
        :class:`Parser`: Self for chaining.
        """
        for __typename in self.typenames:
            converters.pop(__typename, None)
        for __type in self.types:
            adapters.pop((__type, PrepareProtocol), None)
        return self

    def __hash__(self) -> int:
        return hash((*self.typenames, *self.types))


@final
class BuiltinParsers:
    """Container for default and some extra parsers.

    Attributes
    ----------
    BLOB : :class:`Parser` [`bytes`]
        Parser for binary data.
    INT : :class:`Parser` [`int`]
        Parser for integers.
    REAL : :class:`Parser` [`float`]
        Parser for floating-point and real numbers.
    TEXT : :class:`Parser` [`str`]
        Parser for strings.
    BOOL : :class:`Parser` [`bool`]
        Parser for boolean values.
    SET : :class:`Parser` [`set`]
        Parser for python sets.
    LIST : :class:`Parser` [`list`]
        Parser for python lists.
    TUPLE : :class:`Parser` [`tuple`]
        Parser for python tuples.
    DICT : :class:`Parser` [`dict`]
        Parser for python dictionaries.
    JSON : :class:`Parser` [`dict` | `list`]
        Parser for both python dictionaries and lists (AKA JSON objects).
    TIME : :class:`Parser` [`time`]
        Parser for time in one of the `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ formats.
    DATE : :class:`Parser` [`date`]
        Parser for date in `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.
    DATETIME : :class:`Parser` [`datetime`]
        Parser for date and time in `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.

    .. important::
        Do not registrate ``BLOB``, ``INT``, ``REAL`` and ``TEXT`` parsers using
        their's ``register()`` method.

        Before using ``BOOL``, ``SET``, ``LIST``, ``TUPLE``, ``DICT``, ``JSON``, ``TIME``,
        ``DATE`` and ``DATETIME`` parsers, you must call :func:`BuiltinParsers.init()` method.
    
    See Also
    --------
    :class:`Parser`
        Class for creating custom parsers.
    :class:`Parsable`
        Base class for custom parsable objects.
    """
    BLOB: ClassVar[Parser[bytes]] = Parser({bytes}, {'BLOB', 'BYTES'}, bytes, bytes)
    INT: ClassVar[Parser[int]] = Parser({int}, {'INTEGER', 'INT'}, int, int)
    REAL: ClassVar[Parser[float]] = Parser({float}, {'REAL', 'FLOAT'}, float, float)
    TEXT: ClassVar[Parser[str]] = Parser({str}, {'TEXT', 'CHAR', 'VARCHAR'}, str, str)
    BOOL: ClassVar[Parser[bool]]
    SET: ClassVar[Parser[set]]
    LIST: ClassVar[Parser[list]]
    TUPLE: ClassVar[Parser[tuple]]
    DICT: ClassVar[Parser[dict]]
    JSON: ClassVar[Parser[dict | list]]
    TIME: ClassVar[Parser[time]]
    DATE: ClassVar[Parser[date]]
    DATETIME: ClassVar[Parser[datetime]]

    @staticmethod
    def init() -> None:
        """Creates and registrates all builtin parsers except `BLOB`, `INT`, `REAL` and `TEXT` (those were created automatically)."""
        BuiltinParsers.BOOL = Parser({bool}, {'BOOL', 'BOOLEAN'}, lambda data: t == b'true' if (t := data.lower()) in (b'true', b'false') else bool(data), str).register()
        BuiltinParsers.LIST = BuiltinParsers.DICT = BuiltinParsers.JSON = Parser({dict, list}, {'JSON', 'LIST', 'DICT'}, loads, lambda obj: dumps(obj, ensure_ascii=False)).register()
        BuiltinParsers.SET = Parser({set}, {'SET'}, lambda data: set(loads(data)), lambda obj: dumps(tuple(obj), ensure_ascii=False)).register()
        BuiltinParsers.TUPLE = Parser({tuple}, {'TUPLE'}, lambda data: tuple(loads(data)), BuiltinParsers.JSON.dumps).register()
        BuiltinParsers.TIME = Parser({time}, {'TIME'}, time.fromisoformat, time.isoformat).register()
        BuiltinParsers.DATE = Parser({date}, {'DATE'}, date.fromisoformat, date.isoformat).register()
        BuiltinParsers.DATETIME = Parser({datetime}, {'DATETIME'}, datetime.fromisoformat, datetime.isoformat).register()
