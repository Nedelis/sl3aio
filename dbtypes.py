from abc import ABC, abstractmethod
from dataclasses import InitVar, dataclass, field
from types import UnionType
from typing import Optional, Any, Self, Tuple, List, Protocol, Dict, DefaultDict
from collections import defaultdict
from dataparser import DataParser, DefaultDataType
from operator import itemgetter
from executor import Executor, _ExecutorFactory, single_executor, PathLike


class TableChoicePredicate[T](Protocol):
    def __call__(self, record: 'TableRecord[T]') -> bool:
        ...


@dataclass(slots=True, frozen=True, match_args=False)
class TableColumn[T]:
    sql: str
    default: Optional[T] = None
    name: str = field(init=False)
    parser: DataParser[T, DefaultDataType] = field(init=False)

    def __post_init__(self) -> None:
        attrs = self.sql.split(maxsplit=2)
        object.__setattr__(self, 'name', attrs[0])
        object.__setattr__(self, 'parser', DataParser.get_for(attrs[1]))

    @property
    def primary(self) -> bool:
        return 'PRIMARY KEY' in self.sql

    @property
    def unique(self) -> bool:
        return 'UNIQUE' in self.sql
    
    @property
    def nullable(self) -> bool:
        return 'NOT NULL' not in self.sql
    

class TableRecord[T](Dict[str, T]):
    __slots__ = ()

    def asdict(self) -> Dict[str, T]:
        return dict(self)
    
    def astuple(self) -> Tuple[T]:
        return tuple(self.values())

    def __hash__(self) -> int:
        return hash(self.astuple())

    def __getattribute__(self, name: str) -> T | Any:
        return self[name] if name in self else super().__getattribute__(name)
    
    def __setitem__(self, *_) -> None:
        return None
    
    def __setattr__(self, *_) -> None:
        return None
    
    def __or__[OT](self, other: 'TableRecord[OT]') -> 'TableRecord[T | OT]':
        return TableRecord(super().__or__(other))


@dataclass(slots=True, frozen=True)
class TableABC[T](ABC):
    database: PathLike
    name: str
    columns: Tuple[TableColumn[T]]
    _default: TableRecord[T]

    def __post_init__(self) -> None:
        assert self.columns, 'Table must have at least one column!'
        object.__setattr__(
            self,
            '_default',
            TableRecord((column.name, column.default) for column in self.columns)
        )

    @abstractmethod
    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]: ...

    @abstractmethod
    async def select_first(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None: ...

    @abstractmethod
    async def insert(self, record: TableRecord[T]) -> None: ...

    @abstractmethod
    async def pop(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]: ...

    @abstractmethod
    async def pop_first(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None: ...

    @abstractmethod
    async def update(self, source: TableRecord[T], predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def update_first(self, source: TableRecord[T], predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def create(self) -> None: ...

    @abstractmethod
    async def drop(self) -> None: ...


@dataclass(slots=True, frozen=True)
class SQLTable[T](TableABC[T]):
    executors: Dict[str, _ExecutorFactory] = field(default_factory=dict)

    def get_executor(self, function: str) -> Executor:
        return self.executors.get(function, single_executor)(self.database)

    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        cursor = await self.get_executor('select').execute(f'SELECT * FROM {self.name}')
        if not cursor:
            return ()
        return tuple(
            record
            for entry in cursor.fetchall()
            if predicate(record := TableRecord[T](zip(self._default, entry)))
        )
    
    async def select_first(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None:
        cursor = await self.get_executor('select_first').execute(f'SELECT * FROM {self.name}')
        if not cursor:
            return None
        return next((
            record
            for entry in cursor.fetchall()
            if predicate(record := TableRecord[T](zip(self._default, entry)))
        ), None)
    
    async def insert(self, record: TableRecord[T]) -> None:
        await self.get_executor('insert').execute('INSERT OR UPDATE INTO %s (%s) ')


@dataclass(slots=True, frozen=True)
class Table[T](TableABC[T]):
    _records: List[TableRecord[T]] = field(default_factory=list)
    executors: Dict[str, _ExecutorFactory] = field(default_factory=dict)

    def get_executor(self, function: str) -> Executor:
        return self.executors.get(function, single_executor)(self.database)

    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        if not self._records:
            return ()
        return tuple(filter(predicate, self._records) if predicate else self._records)

    async def select_first(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None:
        if not self._records:
            return None
        return next(filter(predicate, self._records), None) if predicate else self._records[0]

    async def insert(self, record: TableRecord[T]) -> None:
        record_ = self._default | record
        self._records.append(record_)
        await self.get_executor('insert').execute(
            'INSERT OR REPLACE INTO TABLE %s (%s) VALUES (%s)' % (
                self.name,
                ', '.join(record_),
                ', '.join('?' * len(record_))
            ),
            record_.astuple()
        )

    async def update(self, source: TableRecord[T], predicate: TableChoicePredicate[T] | None = None) -> None:
        if not (selected := await self.select(predicate)):
            return
        for record in selected:
            dict.update(record, source)
        sql = 'UPDATE %s SET %s WHERE %s' % (
            self.name,
            ', '.join(f'{k} = ?' for k in source),
            ', '.join(f'{k} = ?' for k in self._default)
        )
        factory = self.executors.get('update', single_executor)
        for record in selected:
            await factory(self.database).execute(sql, (
                *source.values(),
                *(record[k] for k in self._default)
            ))
