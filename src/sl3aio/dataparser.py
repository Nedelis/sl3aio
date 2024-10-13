from dataclasses import dataclass, field, InitVar
from json import loads, dumps
from datetime import datetime, date, time
from collections.abc import Callable, Iterable
from typing import ClassVar, Self, final, Protocol
from sqlite3 import register_adapter, register_converter

__all__ = ['DefaultDataType', 'Parser', 'Parsable', 'BuiltinParser']

type DefaultDataType = bytes | str | int | float | None


class Parsable(Protocol):
    @classmethod
    def from_data(cls, data: str) -> Self: ...

    def to_data(self) -> 'DefaultDataType | Parsable': ...


@dataclass(slots=True, frozen=True)
class Parser[T]:
    registry: ClassVar[set['Parser']] = set()
    types: tuple[type[T], ...]
    typenames: tuple[str, ...]
    loads: Callable[[DefaultDataType], T] = field(repr=False)
    dumps: Callable[[T], DefaultDataType] = field(repr=False)
    register: InitVar[bool] = True

    @classmethod
    def from_parsable(cls, parsable: type[Parsable], typenames: Iterable[str] = (), register: bool = True) -> Self:
        return cls(
            (parsable,),
            tuple(typenames) or (parsable.__name__,),
            parsable.from_data,
            parsable.to_data,
            register
        )

    @classmethod
    def get_by_type(cls, __type: T) -> Self | None:
        return next((parser for parser in cls.registry if __type in parser.types), None)
    
    @classmethod
    def get_by_typename(cls, typename: str, case_sensitive: bool = False) -> Self | None:
        if case_sensitive:
            return next((parser for parser in cls.registry if typename in parser.typenames), None)
        typename = typename.lower()
        for parser in cls.registry:
            if typename in (ptypename.lower() for ptypename in parser.typenames):
                return parser

    def __post_init__(self, register: bool) -> None:
        assert self.types, 'Parser must have at least one type corresponding to it!'
        assert self.typenames, 'Parser must have at least one typename corresponding to it!'
        Parser.registry.add(self)
        if register:
            for typename in self.typenames:
                register_converter(typename, self.loads)
            for type in self.types:
                register_adapter(type, self.dumps)


@final
class BuiltinParser:
    BLOB: ClassVar[Parser[bytes]] = Parser((bytes,), ('BLOB', 'BYTES'), bytes, bytes, False)
    INTEGER: ClassVar[Parser[int]] = Parser((int,), ('INTEGER', 'INT'), int, int, False)
    REAL: ClassVar[Parser[float]] = Parser((float,), ('REAL', 'FLOAT'), float, float, False)
    TEXT: ClassVar[Parser[str]] = Parser((str,), ('TEXT', 'CHAR', 'VARCHAR'), str, str, False)
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
        BuiltinParser.BOOL = Parser((bool,), ('BOOL',), lambda data: t == b'true' if (t := data.lower()) in (b'true', b'false') else bool(data), str)
        BuiltinParser.LIST = BuiltinParser.DICT = BuiltinParser.JSON = Parser((dict, list), ('JSON', 'LIST', 'DICT'), loads, lambda obj: dumps(obj, ensure_ascii=False))
        BuiltinParser.SET = Parser((set,), ('SET',), lambda data: set(loads(data)), lambda obj: dumps(tuple(obj), ensure_ascii=False))
        BuiltinParser.TUPLE = Parser((tuple,), ('TUPLE',), lambda data: tuple(loads(data)), BuiltinParser.JSON.dumps)
        BuiltinParser.TIME = Parser((time,), ('TIME',), time.fromisoformat, time.isoformat)
        BuiltinParser.DATE = Parser((date,), ('DATE',), date.fromisoformat, date.isoformat)
        BuiltinParser.DATETIME = Parser((datetime,), ('DATETIME',), datetime.fromisoformat, datetime.isoformat)
