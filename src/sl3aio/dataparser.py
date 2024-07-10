from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field, InitVar
from json import loads, dumps
from datetime import datetime, date, time
from typing import Callable, Type, ClassVar, Set, Tuple, Dict, Any, Self, final
from sqlite3 import register_adapter, register_converter
from ._logging import get_logger

type DefaultDataType = bytes | str | int | float | None

__LOGGER = get_logger()


@dataclass(slots=True, frozen=True)
class Parser[T]:
    registry: ClassVar[Set['Parser']] = set()
    types: Tuple[Type[T], ...]
    aliases: Tuple[str, ...]
    loads: Callable[[bytes], T] = field(repr=False)
    dumps: Callable[[T], DefaultDataType] = field(repr=False)
    register: InitVar[bool] = True

    @staticmethod
    def get_by_type[_T](type: Type[_T]) -> 'Parser[_T] | None':
        return next(filter(
            lambda parser: type in parser.types,
            Parser.registry
        ), None)
    
    @staticmethod
    def get_by_alias(alias: str) -> 'Parser | None':
        return next(filter(
            lambda parser: alias in parser.aliases,
            Parser.registry
        ), None)

    def __post_init__(self, register: bool) -> None:
        if not self.types:
            __LOGGER.error('Parser must have at least one type corresponding to it!')
        elif not self.aliases:
            __LOGGER.error('Parser must have at least one alias corresponding to it!')
        else:
            Parser.registry.add(self)
            if register:
                for alias in self.aliases:
                    register_converter(alias, self.loads)
                for type in self.types:
                    register_adapter(type, self.dumps)


class _ParsableMeta(type, metaclass=ABCMeta):
    def __new__(cls, name: str, bases: Tuple[Type, ...], namespace: Dict[str, Any]) -> 'Parsable':
        subcls: Parsable = super().__new__(cls, name, bases, namespace)
        if name != 'Parsable':
            subcls.parser = Parser(
                (subcls,),
                subcls.aliases if hasattr(subcls, 'aliases') else (subcls.__name__.upper(),),
                lambda data: subcls.fromdict(loads(data)),
                lambda parsable: dumps(parsable.asdict(), ensure_ascii=False)
            )
        return subcls


class Parsable(metaclass=_ParsableMeta):
    parser: ClassVar[Parser[Self]]
    aliases: ClassVar[Tuple[str, ...]]

    @classmethod
    @abstractmethod
    def fromdict[T](cls: Type[T], value: Dict[str, Any]) -> T: ...

    @abstractmethod
    def asdict(self) -> Dict[str, Any]: ...


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
