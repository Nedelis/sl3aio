from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Any, Tuple, List, Protocol, Dict
from dataparser import DataParser, DefaultDataType
from executor import Executor, _ExecutorFactory, PathLike, deferred_executor


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
    _default: TableRecord[T] = field(init=False)

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
    async def select_one(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None: ...

    @abstractmethod
    async def insert(self, *records: TableRecord[T]) -> None: ...

    @abstractmethod
    async def insert_one(self, record: TableRecord[T]) -> None: ...

    @abstractmethod
    async def remove(self, predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def remove_one(self, predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def update(self, source: TableRecord[T], predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def update_one(self, source: TableRecord[T], predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def create(self) -> None: ...

    @abstractmethod
    async def drop(self) -> None: ...


@dataclass(slots=True, frozen=True)
class MemoizedTable[T](TableABC[T]):
    _records: List[TableRecord[T]] = field(default_factory=list)
    executors: Dict[str, _ExecutorFactory] = field(default_factory=dict)

    def get_executor(self, function: str) -> Executor:
        return self.executors.get(function, deferred_executor)(self.database)

    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        if not self._records:
            return ()
        return tuple(filter(predicate, self._records) if predicate else self._records)

    async def select_one(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None:
        if not self._records:
            return None
        return next(filter(predicate, self._records), None) if predicate else self._records[0]

    async def insert(self, *records: TableRecord[T]) -> None:
        for record in records:
            await self.insert_one(record)

    async def insert_one(self, record: TableRecord[T]) -> None:
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

    async def remove(self, predicate: TableChoicePredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            self._records.clear()
            await self.get_executor('remove').execute(f'DELETE FROM {self.name}')
            return
        selected: List[TableRecord[T]] = []
        for record in tuple(self._records):
            if predicate(record):
                selected.append(record)
                self._records.remove(record)
        factory = self.executors.get('remove', deferred_executor)
        for record in selected:
            await factory(self.database).execute(
                'DELETE FROM %s WHERE %s' % (
                    self.name,
                    ', '.join(f'{k} = ?' for k in record)
                ),
                record.astuple()
            )
    
    async def remove_one(self, predicate: TableChoicePredicate[T] | None = None) -> None:
        if (selected := await self.select_one(predicate)):
            self._records.remove(selected)
            await self.get_executor('remove_one').execute(
                'DELETE FROM %s WHERE %s' % (
                    self.name,
                    ', '.join(f'{k} = ?' for k in selected)
                ),
                selected.astuple()
            )

    async def update(self, source: TableRecord[T], predicate: TableChoicePredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            for record in self._records:
                dict.update(record, source)
            await self.get_executor('update').execute(
                'UPDATE %s SET %s' % (
                    self.name,
                    ', '.join(f'{k} = ?' for k in source)
                ),
                source.astuple()
            )
        selected: List[TableRecord[T]] = []
        for record in self._records:
            if predicate(record):
                dict.update(record, source)
                selected.append(record)
        factory = self.executors.get('update', deferred_executor)
        sql = 'UPDATE %s SET %s WHERE ' % (
            self.name,
            ', '.join(f'{k} = ?' for k in source)
        )
        for record in selected:
            await factory(self.database).execute(
                sql + ', '.join(f'{k} = ?' for k in record),
                source.astuple() + record.astuple()
            )

    async def update_one(self, source: TableRecord[T], predicate: TableChoicePredicate[T] | None = None) -> None:
        if (selected := await self.select_one(predicate)):
            dict.update(selected, source)
            await self.get_executor('update_one').execute(
                'UPDATE %s SET %s WHERE %s' % (
                    self.name,
                    ', '.join(f'{k} = ?' for k in source),
                    ', '.join(f'{k} = ?' for k in selected)
                ),
                source.astuple() + selected.astuple()
            )

    async def create(self) -> None:
        await self.drop()
        await self.get_executor('create').execute('CREATE TABLE %s (%s)' % (
            self.name,
            ', '.join(column.sql for column in self.columns)
        ))
        records = tuple(self._records)
        self._records.clear()
        await self.insert(*records)

    async def drop(self) -> None:
        await self.get_executor('drop').execute(f'DROP TABLE IF EXISTS {self.name}')


@dataclass(slots=True, frozen=True)
class MemoryTable[T](TableABC[T]):
    _records: List[TableRecord[T]] = field(default_factory=list)

    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        if not self._records:
            return ()
        return tuple(filter(predicate, self._records) if predicate else self._records)

    async def select_one(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None:
        if not self._records:
            return None
        return next(filter(predicate, self._records), None) if predicate else self._records[0]

    async def insert(self, *records: TableRecord[T]) -> None:
        self._records.extend(records)
    
    async def insert_one(self, record: TableRecord[T]) -> None:
        return await super().insert_one(record)
