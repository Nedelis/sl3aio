from dataclasses import dataclass, field, InitVar
from json import loads, dumps
from datetime import datetime, date, time
from typing import Callable, Type, ClassVar, Set, Tuple, Dict, Any, final, Protocol
from sqlite3 import register_adapter, register_converter
from ._logging import get_logger

type DefaultDataType = bytes | str | int | float | None

__LOGGER = get_logger('dataparser')


@dataclass(slots=True, frozen=True)
class Parser[T]:
    registry: ClassVar[Set['Parser']] = set()
    types: Tuple[Type[T], ...]
    typenames: Tuple[str, ...]
    loads: Callable[[DefaultDataType], T] = field(repr=False)
    dumps: Callable[[T], DefaultDataType] = field(repr=False)
    register: InitVar[bool] = True

    @staticmethod
    def get_by_type[_T](type: Type[_T]) -> 'Parser[_T] | None':
        return next(filter(
            lambda parser: type in parser.types,
            Parser.registry
        ), None)
    
    @staticmethod
    def get_by_typename(typename: str) -> 'Parser | None':
        return next(filter(
            lambda parser: typename in parser.typenames,
            Parser.registry
        ), None)

    def __post_init__(self, register: bool) -> None:
        global __LOGGER
        if not self.types:
            __LOGGER.error('Parser must have at least one type corresponding to it!')
        elif not self.typenames:
            __LOGGER.error('Parser must have at least one typename corresponding to it!')
        else:
            Parser.registry.add(self)
            if register:
                for typename in self.typenames:
                    register_converter(typename, self.loads)
                for type in self.types:
                    register_adapter(type, self.dumps)


class Parsable(Protocol):
    @classmethod
    def fromdict[T](cls: Type[T], value: Dict[str, Any]) -> T: ...

    def asdict(self) -> Dict[str, Any]: ...


def parsable[T: Parsable](typenames: Tuple[str, ...] | Type[T] = (), register_parser: bool = True) -> Type[T] | Callable[[Type[T]], Type[T]]:
    def decorator(cls: Type[T]) -> Type[T]:
        cls.parser = Parser(
            (cls,),
            typenames,
            lambda data: cls.fromdict(loads(data)),
            lambda obj: dumps(obj.asdict(), ensure_ascii=False),
            register_parser
        )
        return cls
    if isinstance(typenames, tuple):
        return decorator
    _cls, typenames = typenames, (typenames.__name__.upper(),)
    return decorator(_cls)


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
