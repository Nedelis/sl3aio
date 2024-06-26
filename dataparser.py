from dataclasses import dataclass
from json import loads, dumps
from datetime import datetime, date, time
from typing import ClassVar, Dict, Callable, List, Self

type DefaultDataType = bytes | str | int | float | None


@dataclass(slots=True)
class DataParser[OT, DT: DefaultDataType]:
    __parsers: ClassVar[Dict[str, Self]] = {}
    alias: str
    loads: Callable[[DT], OT] = None
    dumps: Callable[[OT], DT] = None

    def __post_init__(self) -> None:
        if self.loads is None:
            @self.set_loads
            def _(data: DT) -> OT: return data
        if self.dumps is None:
            @self.set_dumps
            def _(obj: OT) -> DT: return obj
        DataParser.__parsers[self.alias] = self

    def set_loads(self, loads: Callable[[DT], OT]) -> Callable[[DT], OT]:
        self.loads = loads
        return loads
    
    def set_dumps(self, dumps: Callable[[OT], DT]) -> Callable[[OT], DT]:
        self.dumps = dumps
        return dumps

    @staticmethod
    def get_for(alias: str) -> 'DataParser':
        return DataParser.__parsers.get(alias, NUMERIC)


NUMERIC = DataParser('NUMERIC')
INTEGER = DataParser[int, int]('INTEGER')
REAL = DataParser[float, float]('REAL')
TEXT = DataParser[str, str]('TEXT')
BLOB = DataParser[bytes, bytes]('BLOB')
NULL = DataParser[None, None]('NULL')
JSON = DataParser[Dict | List, str]('JSON', loads, lambda obj: dumps(obj, ensure_ascii=False))
DATE = DataParser[date, str]('DATE', date.fromisoformat, date.isoformat)
TIME = DataParser[time, str]('TIME', time.fromisoformat, time.isoformat)
DATETIME = DataParser[datetime, str]('DATETIME', datetime.fromisoformat, datetime.isoformat)
