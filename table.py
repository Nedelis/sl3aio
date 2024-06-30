from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional, Any, Tuple, ClassVar, Protocol, Dict, Self, Type, Set, Iterator
from collections import namedtuple
from executor import Executor, _ExecutorFactory, PathLike, deferred_executor


class TableRecord[T](Protocol):
    nonrepeating: ClassVar[Tuple[str, ...]]
    _fields: ClassVar[Tuple[str, ...]]

    def __new__(cls, *args: T, **kwargs: T) -> Self: ...

    def _replace(self, **to_replace: T) -> Self: ...

    def _asdict(self) -> Dict[str, T]: ...

    def __getattribute__(self, name: str) -> T | Any: ...

    def __getitem__(self, key: str | int) -> T: ...

    def __hash__(self) -> int: ...

    def __eq__(self) -> bool: ...

    def __iter__(self) -> Iterator[T]: ...

    def __contains__(self, value: T) -> bool: ...

    def __len__(self) -> int: ...


class TableChoicePredicate[T](Protocol):
    def __call__(self, record: 'TableRecord[T]') -> bool:
        ...


@dataclass(slots=True, frozen=True, match_args=False)
class TableColumn[T]:
    sql: str
    default: Optional[T] = None
    name: str = field(init=False)
    datatype: str = field(init=False)

    def __post_init__(self) -> None:
        attrs = self.sql.split(maxsplit=2)
        object.__setattr__(self, 'name', attrs[0])
        object.__setattr__(self, 'datatype', attrs[1])

    @property
    def primary(self) -> bool:
        return 'PRIMARY KEY' in self.sql

    @property
    def unique(self) -> bool:
        return 'UNIQUE' in self.sql
    
    @property
    def nullable(self) -> bool:
        return 'NOT NULL' not in self.sql


@dataclass(slots=True, frozen=True)
class TableABC[T](ABC):
    name: str
    columns: Tuple[TableColumn[T], ...]
    _record_factory: Type[TableRecord[T]] = field(init=False, repr=False, hash=False, compare=False)

    def __post_init__(self) -> None:
        assert self.columns, 'Table must have at least one column!'
        object.__setattr__(
            self,
            '_record_factory',
            table_record(*self.columns, table_name=self.name)
        )

    @abstractmethod
    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]: ...

    @abstractmethod
    async def select_one(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None: ...

    @abstractmethod
    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None: ...

    @abstractmethod
    async def remove(self, predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def remove_one(self, predicate: TableChoicePredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def update(self, predicate: TableChoicePredicate[T] | None = None, **to_update: T) -> None: ...

    @abstractmethod
    async def update_one(self, predicate: TableChoicePredicate[T] | None = None, **to_update: T) -> None: ...

    async def pop(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        selected = await self.select(predicate)
        await self.remove(predicate)
        return selected
    
    async def pop_one(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None:
        selected = await self.select_one(predicate)
        await self.remove_one(predicate)
        return selected

    async def create(self) -> None:
        return

    async def drop(self) -> None:
        return


@dataclass(slots=True, frozen=True)
class MemoryTable[T](TableABC[T]):
    _records: Set[TableRecord[T]] = field(default_factory=set)

    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        if not self._records:
            return ()
        return tuple(filter(predicate, self._records) if predicate else self._records)

    async def select_one(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None:
        if not self._records:
            return None
        return next(filter(predicate, self._records) if predicate else iter(self._records), None)

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = self._record_factory(**values)
        if record in self._records and not ignore_on_repeat:
            self._records.discard(record)
        self._records.add(record)

    async def remove(self, predicate: TableChoicePredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            self._records.clear()
            return
        for record in self._records.copy():
            if predicate(record):
                self._records.discard(record)
    
    async def remove_one(self, predicate: TableChoicePredicate[T] | None = None) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)

    async def update(self, predicate: TableChoicePredicate[T] | None = None, **to_update: T) -> None:
        if not self._records:
            return
        elif not predicate:
            object.__setattr__(
                self,
                '_records',
                {record._replace(**to_update) for record in self._records}
            )
            return
        for record in self._records.copy():
            if predicate(record):
                self._records.discard(record)
                self._records.add(record._replace(**to_update))

    async def update_one(self, predicate: TableChoicePredicate[T] | None = None, **to_update: T) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)
            self._records.add(record._replace(**to_update))


@dataclass(slots=True, frozen=True)
class MemoizedTable[T](TableABC[T]):
    database: PathLike
    _records: Set[TableRecord[T]] = field(default_factory=set)
    executors: Dict[str, _ExecutorFactory] = field(default_factory=dict)

    def _executor(self, function_name: str) -> Executor:
        return self.executors.get(function_name, deferred_executor)(self.database)

    async def select(self, predicate: TableChoicePredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        if not self._records:
            return ()
        return tuple(filter(predicate, self._records) if predicate else self._records)

    async def select_one(self, predicate: TableChoicePredicate[T] | None = None) -> TableRecord[T] | None:
        if not self._records:
            return None
        return next(filter(predicate, self._records), None) if predicate else self._records.pop()

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = self._record_factory(**values)
        if record in self._records and not ignore_on_repeat:
            self._records.discard(record)
        self._records.add(record)
        await self._executor('insert')(
            'INSERT OR REPLACE INTO %s (%s) VALUES (%s)' % (
                self.name,
                ', '.join(record._fields),
                ', '.join('?' * len(record))
            ),
            record
        )

    async def remove(self, predicate: TableChoicePredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            self._records.clear()
            await self._executor('remove')(f'DELETE FROM {self.name}')
            return
        selected: Set[TableRecord[T]] = set()
        for record in self._records.copy():
            if predicate(record):
                self._records.discard(record)
                selected.add(record)
        factory = self.executors.get('remove', deferred_executor)
        for record in selected:
            await factory(self.database)(
                'DELETE FROM %s WHERE %s' % (
                    self.name,
                    ' AND '.join(f'{k} = ?' for k in record._fields)
                ),
                record
            )
    
    async def remove_one(self, predicate: TableChoicePredicate[T] | None = None) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)
            await self._executor('remove')(
                'DELETE FROM %s WHERE %s' % (
                    self.name,
                    ' AND '.join(f'{k} = ?' for k in record._fields)
                ),
                record
            )

    async def update(self, predicate: TableChoicePredicate[T] | None = None, **to_update: T) -> None:
        if not self._records:
            return
        elif not predicate:
            object.__setattr__(
                self,
                '_records',
                {record._replace(**to_update) for record in self._records}
            )
            await self._executor('update')(
                'UPDATE %s SET %s' % (
                    self.name,
                    ', '.join(f'{k} = ?' for k in to_update)
                ),
                to_update.values()
            )
            return
        selected: Set[TableRecord[T]] = set()
        for record in self._records.copy():
            if predicate(record):
                self._records.discard(record)
                self._records.add(record._replace(**to_update))
                selected.add(record)
        factory = self.executors.get('update', deferred_executor)
        sql = 'UPDATE %s SET %s WHERE ' % (
            self.name,
            ', '.join(f'{k} = ?' for k in to_update)
        )
        for record in selected:
            await factory(self.database)(
                sql + ' AND '.join(f'{k} = ?' for k in record._fields),
                (*to_update.values(), *record)
            )

    async def update_one(self, predicate: TableChoicePredicate[T] | None = None, **to_update: T) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)
            self._records.add(record._replace(**to_update))
            await self._executor('update')(
                'UPDATE %s SET %s WHERE %s' % (
                    self.name,
                    ', '.join(f'{k} = ?' for k in to_update),
                    ' AND '.join(f'{k} = ?' for k in record._fields)
                ),
                (*to_update.values(), *record)
            )

    async def create(self) -> None:
        await self.drop()
        await self._executor('create')('CREATE TABLE %s (%s)' % (
            self.name,
            ', '.join(column.sql for column in self.columns)
        ))
        records = self._records.copy()
        self._records.clear()
        for record in records:
            await self.insert(**record._asdict())

    async def drop(self) -> None:
        await self._executor('drop')(f'DROP TABLE IF EXISTS {self.name}')


def table_record[T](*columns: TableColumn[T], table_name: str = '') -> Type[TableRecord[T]]:
    names, nonrepeating, defaults = [], [], []
    for col in columns:
        names.append(col.name)
        defaults.append(col.default)
        if col.unique or col.primary:
            nonrepeating.append(col.name)
    return type(
        f'{table_name}_Record',
        (namedtuple(f'{table_name}_RecordBase', names, defaults=defaults),),
        {
            '__hash__': lambda self: hash(tuple(getattr(self, key) for key in self.nonrepeating)),
            '__eq__': lambda self, other: all(getattr(self, key) == getattr(other, key) for key in self.nonrepeating) if self.nonrepeating else False,
            'nonrepeating': tuple(nonrepeating)
        }
    )
