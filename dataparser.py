from json import loads, dumps
from datetime import datetime, date, time
from typing import Callable, Type
from sqlite3 import register_adapter, register_converter

type DefaultDataType = bytes | str | int | float | None


def register_parser[T](type_: Type[T], alias: str, loads: Callable[[bytes], T], dumps: Callable[[T], DefaultDataType]) -> None:
    register_converter(alias, loads)
    register_adapter(type_, dumps)


def init_inbuilt_parsers() -> None:
    register_parser(dict, 'JSON', loads, dumps)
    register_parser(list, 'JSON', loads, dumps)
    register_parser(time, 'TIME', time.fromisoformat, time.isoformat)
    register_parser(date, 'DATE', date.fromisoformat, date.isoformat)
    register_parser(datetime, 'DATETIME', datetime.fromisoformat, datetime.isoformat)
