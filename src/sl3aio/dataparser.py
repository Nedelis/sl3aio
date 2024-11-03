from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from json import loads, dumps
from datetime import datetime, date, time
from collections.abc import Callable, Iterable
from typing import ClassVar, Self, final
from sqlite3 import adapters, converters, PrepareProtocol

__all__ = ['DefaultDataType', 'Parser', 'Parsable', 'BuiltinParsers']

type DefaultDataType = bytes | str | int | float | None


class Parsable(ABC):
    @classmethod
    @abstractmethod
    def from_data(cls, data: bytes) -> Self: ...

    @abstractmethod
    def to_data(self) -> DefaultDataType | Self: ...


@dataclass(slots=True)
class Parser[T]:
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


@final
class BuiltinParsers:
    """Container for default and some useful parsers.  
    **IMPORTANT!** Do not registrate `BLOB`, `INT`, `REAL` and `TEXT` parsers using their's `register()` method.  
    
    ### Default parsers
    - `BLOB` - parser for binary data
    - `INT` - parser for integers
    - `REAL` - parser for floating-point numbers
    - `TEXT` - parser for strings

    ### Extra parsers
    Before using these call `BuiltinParsers.init()` method.
    - `BOOL` - parser for boolean values
    - `SET` - parser for python sets
    - `LIST` - parser for python lists
    - `TUPLE` - parser for python tuples
    - `DICT` - parser for python dictionaries
    - `JSON` - parser for both dictionaries and lists
    - `TIME` - parser for time in one of the iso 8601 formats
    - `DATE` - parser for date in iso 8601 format
    - `DATETIME` - parser for date and time in iso 8601 format
    
    ### See also
    - `sl3aio.Parser` - class for creating custom parsers
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
