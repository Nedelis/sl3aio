from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from json import loads, dumps
from datetime import datetime, date, time
from collections.abc import Callable, Iterable
from operator import call
from typing import ClassVar, Self, final
from sqlite3 import adapters, converters, PrepareProtocol

__all__ = ['DefaultDataType', 'allowed_types', 'allowed_typenames', 'Parser', 'Parsable', 'BuiltinParsers']

type DefaultDataType = bytes | str | int | float | None
""":class:`TypeAlias`: types that are supported by sqlite3 natively."""


def allowed_types() -> set[type]:
    """List types that can be written into database.
    
    Returns
    -------
    :obj:`set[type]`: set of writable types
    """
    return {bytes, str, int, float, None, *(k[0] for k in adapters)}


def allowed_typenames() -> set[str]:
    """List names that can be used as column type in database.

    .. note::
        For **default** data types, this list includes only their affinities. So there are no
        such typenames as `DOUBLE`, `TINYINT`, `VARCHAR(...)` and etc. in the result set.
    
    Returns
    -------
    :obj:`set[str]`: set of allowed columns types
    """
    return {'BLOB', 'TEXT', 'INTEGER', 'REAL', 'NUMERIC', *converters}


class Parsable(ABC):
    """Base class for custom parsable objects.

    Methods
    -------
    from_data(data)
        Constructs self from binary data.
    to_data()
        Converts self to any instance of the allowed type (listed by `allowed_types()` method).
    
    See Also
    --------
    :obj:`Parser`: Class for creating custom parsers.
    """
    @classmethod
    @abstractmethod
    def from_data(cls, data: bytes) -> Self:
        """Create an instance from a binary data.
        
        Parameters
        ----------
        data : bytes
            Incoming data from the sqlite database.

        Returns
        -------
        :obj:`Parsable`: instance of the parsable class.
        """

    @abstractmethod
    def to_data(self) -> 'DefaultDataType | Parsable':
        """Convert self to the object that is writable by sqlite.
        
        Returns
        -------
        :obj:`DefaultDataType | Parsable`: object that can be written into sqlite database.
        """


@dataclass(slots=True)
class Parser[T]:
    """Class for creating custom parsers.
    
    Automates the registration of converters and adapters in sqlite3. Provides
    convinient access to the already registered parsers.

    Attributes
    ----------
    instances : set[Parsable]
        Container for all of the parsers that were created.
    types : set[type[T]]
        Set of types corresponding to the parser.
    typenames : set[str]
        Set of names corresponding to the parser.
    loads : Callable[[bytes], T]
        Method to parse a data from bytes.
    dumps : Callable[[T], DefaultDataType | Parsable]
        Method to convert an object to the `DefaultDataType` or `Parsable` object.
    
    Methods
    -------
    """
    instances: ClassVar[set[Self]] = set()
    types: set[type[T]]
    typenames: set[str]
    loads: Callable[[bytes], T] = field(repr=False)
    dumps: Callable[[T], DefaultDataType | Parsable] = field(repr=False)

    @classmethod
    def from_parsable(cls, parsable: type[Parsable], typenames: Iterable[str] = ()) -> Self:
        return cls(
            {parsable},
            set(map(str.upper, typenames)) or {parsable.__name__.upper()},
            parsable.from_data,
            parsable.to_data
        )

    @classmethod
    def get_by_type(cls, __type: T) -> Self | None:
        return next((parser for parser in cls.instances if __type in parser.types), None)
    
    @classmethod
    def get_by_typename(cls, __typename: str) -> Self | None:
        __typename = __typename.upper()
        return next((parser for parser in cls.instances if __typename in parser.typenames), None)

    def __post_init__(self) -> None:
        assert self.types, 'Parser must have at least one type corresponding to it!'
        assert self.typenames, 'Parser must have at least one typename corresponding to it!'
        self.instances.add(self)

    def register(self) -> Self:
        for __typename in self.typenames:
            converters[__typename] = self.loads
        for __type in self.types:
            adapters[(__type, PrepareProtocol)] = self.dumps
        return self

    def unregister(self) -> Self:
        for __typename in self.typenames:
            converters.pop(__typename, None)
        for __type in self.types:
            adapters.pop((__type, PrepareProtocol), None)
        return self

    def __hash__(self) -> int:
        return hash((*self.typenames, *self.types))


@call
@final
class BuiltinParsers:
    """Container for default and some extra parsers.

    Attributes
    ----------
    BLOB : Parser[bytes]
        Parser for binary data.
    INT : Parser[int]
        Parser for integers.
    REAL : Parser[float]
        Parser for floating-point and real numbers.
    TEXT : Parser[str]
        Parser for strings.
    BOOL : Parser[bool]
        Parser for boolean values.
    SET : Parser[set]
        Parser for python sets.
    LIST : Parser[list]
        Parser for python lists.
    TUPLE : Parser[tuple]
        Parser for python tuples.
    DICT : Parser[dict]
        Parser for python dictionaries.
    JSON : Parser[dict | list]
        Parser for both python dictionaries and lists (AKA JSON objects).
    TIME : Parser[time]
        Parser for time in one of the `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ formats.
    DATE : Parser[date]
        Parser for date in `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.
    DATETIME : Parser[datetime]
        Parser for date and time in `iso 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_ format.

    .. attention::
        Do not registrate `BLOB`, `INT`, `REAL` and `TEXT` parsers using
        their's ``register()`` method!

    .. important::
        Before using `BOOL`, `SET`, `LIST`, `TUPLE`, `DICT`, `JSON`, `TIME`, `DATE` and
        `DATETIME` parsers, you must call `BuiltinParsers.init()` method.

    Methods
    -------
    init()
        Initializes extra parsers.
    
    See Also
    --------
    :class:`Parser`: Class for creating custom parsers.
    :class:`Parsable`: Base class for custom parsable objects.
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
