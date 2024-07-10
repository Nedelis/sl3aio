from abc import ABC, abstractmethod
from re import IGNORECASE, search
from os import PathLike
from os.path import abspath
from dataclasses import dataclass, field
from typing import Optional, Any, Tuple, ClassVar, Protocol, Dict, Self, Type, Set, Iterator, Sequence
from collections import namedtuple
from .executor import Executor, _ExecutorFactory, deferred_executor, single_executor, Cursor


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


class TableSelectPredicate[T](Protocol):
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
class Table[T](ABC):
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
    async def contains(self, record: TableRecord[T]) -> bool: ...

    @abstractmethod
    async def select(self, predicate: TableSelectPredicate[T] | None = None) -> Tuple[TableRecord[T], ...]: ...

    @abstractmethod
    async def select_one(self, predicate: TableSelectPredicate[T] | None = None) -> TableRecord[T] | None: ...

    @abstractmethod
    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None: ...

    @abstractmethod
    async def delete(self, predicate: TableSelectPredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def delete_one(self, predicate: TableSelectPredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def update(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None: ...

    @abstractmethod
    async def update_one(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None: ...

    async def pop(self, predicate: TableSelectPredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        selected = await self.select(predicate)
        await self.delete(predicate)
        return selected
    
    async def pop_one(self, predicate: TableSelectPredicate[T] | None = None) -> TableRecord[T] | None:
        selected = await self.select_one(predicate)
        await self.delete_one(predicate)
        return selected

    async def create(self) -> None:
        return

    async def drop(self) -> None:
        return


@dataclass(slots=True, frozen=True)
class MemoryTable[T](Table[T]):
    _records: Set[TableRecord[T]] = field(default_factory=set)

    async def contains(self, record: TableRecord[T]) -> bool:
        return record in self._records

    async def select(self, predicate: TableSelectPredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        if not self._records:
            return ()
        return tuple(filter(predicate, self._records) if predicate else self._records)

    async def select_one(self, predicate: TableSelectPredicate[T] | None = None) -> TableRecord[T] | None:
        if not self._records:
            return None
        return next(filter(predicate, self._records) if predicate else iter(self._records), None)

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = self._record_factory(**values)
        if record in self._records and not ignore_on_repeat:
            self._records.discard(record)
        self._records.add(record)

    async def delete(self, predicate: TableSelectPredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            self._records.clear()
            return
        for record in self._records.copy():
            if predicate(record):
                self._records.discard(record)
    
    async def delete_one(self, predicate: TableSelectPredicate[T] | None = None) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)

    async def update(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None:
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

    async def update_one(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)
            self._records.add(record._replace(**to_update))


@dataclass(slots=True, frozen=True)
class SQLTable[T](Table[T], ABC):
    database: PathLike
    _executor_factory: _ExecutorFactory = field(default=single_executor)
    _default_selector: str = field(init=False)
    
    @classmethod
    @abstractmethod
    async def from_database(cls, name: str, database: PathLike, executor_factory: _ExecutorFactory = single_executor) -> Self: ...

    def __post_init__(self) -> None:
        Table.__post_init__(self)
        object.__setattr__(self, 'database', abspath(self.database))
        object.__setattr__(self, '_default_selector', 'WHERE ' + ' AND '.join(f'{k} = ?' for k in self._record_factory._fields))

    @property
    def _executor(self) -> Executor:
        return self._executor_factory(self.database)

    async def _execute_where(self, record: TableRecord[T], query: str, parameters: Sequence[Any] = (), **conn_kwargs: Any) -> Cursor | None:
        if self._record_factory.nonrepeating:
            key = self._record_factory.nonrepeating[0]
            return await self._executor(query + f' WHERE {key} = ?', (*parameters, getattr(record, key)), **conn_kwargs)
        elif None in record:
            values = {key: value for key in self._record_factory._fields if (value := getattr(record, key)) is not None}
            return await self._executor(query + ' WHERE ' + ' AND '.join(f'{key} = ?' for key in values), (*parameters, *values.values()), **conn_kwargs)
        return await self._executor(query + ' ' + self._default_selector, (*parameters, *record), **conn_kwargs)
    
    async def run_executor(self) -> None:
        return await Executor._instances[self.database].run()


@dataclass(slots=True, frozen=True)
class SolidTable[T](SQLTable[T]):
    @classmethod
    async def from_database(cls, name: str, database: PathLike, executor_factory: _ExecutorFactory = single_executor) -> Self:
        return SolidTable(
            name,
            tuple(
                TableColumn(sql, default[0])
                for sql, default in zip(
                    search(
                        r'CREATE TABLE\s+\w+\s*\((.*)\)',
                        (await single_executor(database)(f'SELECT sql FROM sqlite_master WHERE type = "table" AND name = "{name}"')).fetchone()[0],
                        IGNORECASE
                    ).group(1).split(','),
                    await single_executor(database)(f'SELECT dflt_value FROM pragma_table_info("{name}")')
                )
            ),
            database,
            executor_factory
        )
    
    async def contains(self, record: TableRecord[T]) -> bool:
        cursor = await self._execute_where(record, f'SELECT * FROM {self.name}')
        return bool(cursor.fetchone()) if cursor else False

    async def select(self, predicate: TableSelectPredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        cursor = (await self._executor(f'SELECT * FROM {self.name}'))
        return tuple(
            (record for values in cursor if predicate(record := self._record_factory(*values)))
            if predicate else
            (self._record_factory(*values) for values in cursor)
        ) if cursor else ()

    async def select_one(self, predicate: TableSelectPredicate[T] | None = None) -> TableRecord[T] | None:
        cursor = (await self._executor(f'SELECT * FROM {self.name} LIMIT 1'))
        return next(
            (record for values in cursor if predicate(record := self._record_factory(*values)))
            if predicate else
            (self._record_factory(*values) for values in cursor),
            None
        ) if cursor else None

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = self._record_factory(**values)
        await self._executor(
            'INSERT OR %s INTO %s VALUES (%s)' % (
                'IGNORE' if ignore_on_repeat else 'REPLACE',
                self.name,
                ', '.join('?' * len(record))
            ),
            record
        )

    async def delete(self, predicate: TableSelectPredicate[T] | None = None) -> None:
        if not predicate:
            await self._executor(f'DELETE FROM {self.name}')
            return
        for record in self.select(predicate):
            await self._execute_where(record, f'DELETE FROM {self.name}')

    async def delete_one(self, predicate: TableSelectPredicate[T] | None = None) -> None:
        if record := await self.select_one(predicate):
            await self._execute_where(record, f'DELETE FROM {self.name}')

    async def update(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None:
        sql = 'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update))
        if not predicate:
            await self._executor(sql, to_update.values())
            return
        for record in self.select(predicate):
            await self._execute_where(record, sql, to_update.values())

    async def update_one(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None:
        if record := self.select_one(predicate):
            await self._execute_where(
                record,
                'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update)),
                to_update.values()
            )

    async def create(self) -> None:
        await self.drop()
        await self._executor('CREATE TABLE %s (%s)' % (self.name, ', '.join(column.sql for column in self.columns)))

    async def drop(self) -> None:
        await self._executor(f'DROP TABLE IF EXISTS {self.name}')


@dataclass(slots=True, frozen=True)
class MemoizedTable[T](SQLTable[T]):
    _records: Set[TableRecord[T]] = field(default_factory=set)
    _executor_factory: _ExecutorFactory = field(default=deferred_executor)

    @classmethod
    async def from_database(cls, name: str, database: PathLike, executor_factory: _ExecutorFactory = deferred_executor) -> Self:
        table = MemoizedTable(
            name,
            tuple(
                TableColumn(sql, default[0])
                for sql, default in zip(
                    search(
                        r'CREATE TABLE\s+\w+\s*\((.*)\)',
                        (await single_executor(database)(f'SELECT sql FROM sqlite_master WHERE type = "table" AND name = "{name}"')).fetchone()[0],
                        IGNORECASE
                    ).group(1).split(','),
                    await single_executor(database)(f'SELECT dflt_value FROM pragma_table_info("{name}")')
                )
            ),
            database,
            executor_factory
        )
        if cursor := await single_executor(database)(f'SELECT * FROM {name}'):
            object.__setattr__(table, '_records', set(map(table._record_factory, cursor)))
        return table

    async def contains(self, record: TableRecord[T]) -> bool:
        return record in self._records

    async def select(self, predicate: TableSelectPredicate[T] | None = None) -> Tuple[TableRecord[T], ...]:
        return tuple(filter(predicate, self._records) if predicate else self._records) if self._records else ()

    async def select_one(self, predicate: TableSelectPredicate[T] | None = None) -> TableRecord[T] | None:
        return next(filter(predicate, self._records) if predicate else iter(self._records), None) if self._records else None

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = self._record_factory(**values)
        if record in self._records and not ignore_on_repeat:
            self._records.discard(record)
        self._records.add(record)
        await self._executor(
            'INSERT OR %s INTO %s VALUES (%s)' % (
                'IGNORE' if ignore_on_repeat else 'REPLACE',
                self.name,
                ', '.join('?' * len(record))
            ),
            record
        )

    async def delete(self, predicate: TableSelectPredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            self._records.clear()
            await self._executor(f'DELETE FROM {self.name}')
            return
        selected: Set[TableRecord[T]] = set()
        for record in self._records.copy():
            if predicate(record):
                self._records.discard(record)
                selected.add(record)
        for record in selected:
            await self._execute_where(record, f'DELETE FROM {self.name}')
    
    async def delete_one(self, predicate: TableSelectPredicate[T] | None = None) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)
            await self._execute_where(record, f'DELETE FROM {self.name}')

    async def update(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None:
        if not self._records:
            return
        sql = 'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update))
        if not predicate:
            object.__setattr__(self, '_records', {record._replace(**to_update) for record in self._records})
            await self._executor(sql, to_update.values())
            return
        selected: Set[TableRecord[T]] = set()
        for record in self._records.copy():
            if predicate(record):
                self._records.discard(record)
                self._records.add(record._replace(**to_update))
                selected.add(record)
        for record in selected:
            await self._execute_where(record, sql, to_update.values())

    async def update_one(self, predicate: TableSelectPredicate[T] | None = None, **to_update: T) -> None:
        if (record := await self.select_one(predicate)):
            self._records.discard(record)
            self._records.add(record._replace(**to_update))
            await self._execute_where(
                record,
                'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update)),
                to_update.values()
            )

    async def create(self) -> None:
        await self.drop()
        await self._executor('CREATE TABLE %s (%s)' % (self.name, ', '.join(column.sql for column in self.columns)))
        records = self._records.copy()
        self._records.clear()
        for record in records:
            await self.insert(**record._asdict())

    async def drop(self) -> None:
        await self._executor(f'DROP TABLE IF EXISTS {self.name}')


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
            '__eq__': lambda self, other: any(getattr(self, key) == getattr(other, key) for key in self.nonrepeating),
            'nonrepeating': tuple(nonrepeating)
        }
    )
