"""
sl3aio.dataparser
=================

This module provides a flexible and extensible system for parsing and converting data 
between Python objects and SQLite database representations. It offers tools for 
creating custom parsers, handling various data types, and managing the conversion 
process for database operations.

Key Components
--------------
- :class:`Parser`: Class for creating and managing custom data parsers.
- :class:`Parsable`: Abstract base class for creating custom parsable objects.
- :class:`BuiltinParsers`: Container for default and additional pre-defined parsers.

Other Components
----------------
- :obj:`DefaultDataType`: Type alias for basic types natively supported by SQLite.

Features
--------
- Support for native SQLite data types and custom Python objects
- Extensible parser system for handling complex data types
- Automatic registration and management of SQLite adapters and converters
- Built-in parsers for common Python types (e.g., bool, set, list, dict, datetime)
- Utility functions for querying allowed types and type names

Usage
-----
This module is designed to be used in conjunction with the sl3aio library for SQLite 
database operations. It provides the necessary tools to seamlessly convert between 
Python objects and their SQLite representations.

.. Important::
    If you create custom parsers, you should always set the connection's parameter
    ``detect_types`` to ``sqlite3.PARSE_DECLTYPES``.

Examples
--------
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
    `set` [`type`]
        Set of writable types.
    """
    return {bytes, str, int, float, None, *(k[0] for k in adapters)}


def allowed_typenames() -> set[str]:
    """List names that can be used as column type in database.
    
    Returns
    -------
    `set` [`str`]
        Set of allowed columns types.

    .. Note::
        For default data types, this list includes only their affinities. So there are no
        such typenames as ``DOUBLE``, ``TINYINT``, ``VARCHAR(...)`` and etc. in the result set.
    """
    return {'BLOB', 'TEXT', 'INTEGER', 'REAL', 'NUMERIC', *converters}


class Parsable(ABC):
    """Base class for custom parsable objects.
    
    See Also
    --------
    - :class:`Parser`
    """
    @classmethod
    @abstractmethod
    def from_data(cls, data: bytes) -> 'Parsable':
        """Create an instance from a binary data.
        
        Parameters
        ----------
        data : `bytes`
            Incoming data from the sqlite database.

        Returns
        -------
        :class:`Parsable`
            Instance of the parsable class.
        """

    @abstractmethod
    def to_data(self) -> 'Parsable | DefaultDataType':
        """Converts self to the any object of the allowed type, listed in :func:`allowed_types()`.
        
        Returns
        -------
        :class:`Parsable` | :obj:`DefaultDataType`
            Object that can be written into sqlite database.
        """


@dataclass(slots=True)
class Parser[T]:
    """Class for creating custom parsers.
    
    Automates the registration of converters and adapters in sqlite3. Provides
    convinient access to the already registered parsers.

    .. Attention::
        Every single parser must have at least one supported type and at least one
        supported typename, otherwise instantiation will raise the `AssertionError`.
        
    See Also
    --------
    - :class:`Parsable`  
    - :class:`BuiltinParsers`
    """
    instances: ClassVar[set[Self]] = set()
    """Container for all of the parsers that were created."""
    types: set[type[T]]
    """Set of types corresponding to the parser."""
    _typenames: set[str]
    """Set of names corresponding to the parser."""
    loads: Callable[[bytes], T] = field(repr=False)
    """Method to parse a data from bytes."""
    dumps: Callable[[T], Parsable | DefaultDataType] = field(repr=False)
    """Method to convert an object to the object of the allowed type, listed in :func:`allowed_types()`."""

    def __post_init__(self) -> None:
        assert self.types, 'Parser must have at least one type corresponding to it!'
        assert self._typenames, 'Parser must have at least one typename corresponding to it!'
        self._typenames = set(map(str.upper, self._typenames))
        self.instances.add(self)

    @property
    def typenames(self) -> set[str]:
        return self._typenames
    
    @typenames.setter
    def typenames(self, typenames: Iterable[str]) -> None:
        self._typenames = set(map(str.upper, typenames))

    @classmethod
    def from_parsable[T: Parsable](cls, parsable: type[T], typenames: Iterable[str] = ()) -> 'Parser[T]':
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
        `Self`
            New instance of the parser.
        """
        return cls(
            {parsable},
            set(typenames) or {parsable.__name__},
            parsable.from_data,
            parsable.to_data
        )

    @classmethod
    def get_by_type[T](cls, _type: T) -> 'Parser[T] | None':
        """Get an instance of a parser from its registry by the type it supports.
        
        Parameters
        ----------
        _type : `T`
            Type that must be supported by required parser.

        Returns
        -------
        :class:`Parser` [`T`] | `None`
            Instance of the parser or None if no parser corresponding to the given type was found.
        """
        return next((parser for parser in cls.instances if _type in parser.types), None)
    
    @classmethod
    def get_by_typename[T](cls, _typename: str) -> 'Parser[T] | None':
        """Get an instance of a parser from its registry by the typename of type which it supports.
        
        Parameters
        ----------
        _typename : `str`
            Name of the type that must be supported by required parser.
        
        Returns
        -------
        :class:`Parser` [`T`] | `None`
            Instance of the parser or None if no parser corresponding to the given typename was found.
        """
        _typename = _typename.upper()
        return next((parser for parser in cls.instances if _typename in parser.typenames), None)

    def register(self) -> Self:
        """Register converters and adapters in sqlite3.
        
        Returns
        -------
        'Self'
            Self for chaining.
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
        `Self`
            Self for chaining.
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

    .. Attention::
        Do not registrate ``BLOB``, ``INT``, ``REAL`` and ``TEXT`` parsers using
        their's ``register()`` method.

    .. Attention::
        Before using ``BOOL``, ``SET``, ``LIST``, ``TUPLE``, ``DICT``, ``JSON``, ``TIME``,
        ``DATE`` and ``DATETIME`` parsers, you must call :meth:`BuiltinParsers.init()` method.
    
    See Also
    --------
    - :class:`Parser`
    - :class:`Parsable`
    """
    BLOB: ClassVar[Parser[bytes]] = Parser({bytes}, {'BLOB', 'BYTES'}, bytes, bytes)
    """Parser for binary data."""
    INT: ClassVar[Parser[int]] = Parser({int}, {'INTEGER', 'INT'}, int, int)
    """Parser for integers."""
    REAL: ClassVar[Parser[float]] = Parser({float}, {'REAL', 'FLOAT'}, float, float)
    """Parser for floating-point and real numbers."""
    TEXT: ClassVar[Parser[str]] = Parser({str}, {'TEXT', 'CHAR', 'VARCHAR'}, str, str)
    """Parser for strings."""
    BOOL: ClassVar[Parser[bool]]
    """Parser for boolean values."""
    SET: ClassVar[Parser[set]]
    """Parser for python sets."""
    LIST: ClassVar[Parser[list]]
    """Parser for python lists."""
    TUPLE: ClassVar[Parser[tuple]]
    """Parser for python tuples."""
    DICT: ClassVar[Parser[dict]]
    """Parser for python dictionaries."""
    JSON: ClassVar[Parser[dict | list]]
    """Parser for both python dictionaries and lists (JSON objects)."""
    TIME: ClassVar[Parser[time]]
    """Parser for time in one of the `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ formats."""
    DATE: ClassVar[Parser[date]]
    """Parser for date in `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format."""
    DATETIME: ClassVar[Parser[datetime]]
    """Parser for date and time in `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format."""

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
