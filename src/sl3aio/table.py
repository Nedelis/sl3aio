from dataclasses import dataclass, field
from os.path import abspath
from typing import Callable, Awaitable, Type, Dict, ClassVar, Tuple, Any, Self, Protocol, Set, Iterator, Iterable, Never
from inspect import iscoroutinefunction
from abc import ABC, abstractmethod
from asyncio import run, gather
from .executor import Executor, _ExecutorFactory, deferred_executor, single_executor, Cursor
from .dataparser import Parser, BuiltinParser
from ._utils import azip, columns_sql, columns_defaults


class TableRecord[T](tuple[T, ...]):
    __slots__: ClassVar[Tuple] = ()
    table: ClassVar['Table'] = Never
    nonrepeating: ClassVar[Tuple[str, ...]] = Never
    fields: ClassVar[Tuple[str, ...]] = Never

    @classmethod
    def make_subclass(cls, table: 'Table[T]', *columns: 'TableColumn[T]') -> Type[Self]:
        fields, nonrepeating = [], []
        for column in columns:
            fields.append(column.name)
            if column.unique or column.primary:
                nonrepeating.append(column.name)
        return type((table.name.title() if table.name else '') + 'TableRecord', (cls,), {
            'table': table,
            'nonrepeating': tuple(nonrepeating),
            'fields': tuple(fields)
        })

    @classmethod
    async def make(cls, *args: T, **kwargs: T) -> Self:
        params = dict(zip(cls.fields, args)) | kwargs
        result = []
        for column in cls.table.columns:
            value = params[column.name] if column.name in params else await column.get_default(cls.table)
            if value is None and not column.nullable:
                raise ValueError(f'{column.name} cannot be None!')
            result.append(value)
        return super().__new__(cls, result)

    def __new__(cls, *args: T, **kwargs: T) -> Self:
        return run(cls.make(*args, **kwargs))

    def asdict(self) -> Dict[str, T]:
        return dict(zip(self.fields, self))

    def astuple(self) -> Tuple[T, ...]:
        return tuple(self)

    async def replace(self, **to_replace: T) -> Self:
        return await self.make(**(self.asdict() | to_replace))

    def __getattribute__(self, name: str) -> T | Any:
        if name in type(self).fields:
            return self[type(self).fields.index(name)]
        return super().__getattribute__(name)

    def __eq__(self, other: Self) -> bool:
        return any(getattr(self, k) == getattr(other, k) for k in self.nonrepeating)

    def __hash__(self) -> int:
        return hash(tuple(getattr(self, k) for k in self.nonrepeating))


class TableSelectionPredicate[T](Protocol):
    def __call__(self, record: TableRecord[T], table: 'Table[T]') -> bool: ...


@dataclass(slots=True)
class TableColumnValueGenerator[T]:
    instances: ClassVar[Dict[str, Self]] = {}
    generator: Callable[['Table[T]', T | None], T | Awaitable[T]]
    last: T | None = None
    name: str | None = None

    @property
    def is_async(self) -> bool:
        return iscoroutinefunction(self.generator)

    @property
    def uses_last(self) -> bool:
        return self.last is not None

    def register(self, name: str | None = None) -> Self:
        self.name = name or self.name
        self.instances[name or self.name] = self
        return self

    async def next_value(self, table: 'Table[T]') -> T:
        if self.is_async:
            if self.uses_last:
                self.last = await self.generator(table, self.last)
                return self.last
            return await self.generator(table, None)
        if self.uses_last:
            self.last = self.generator(table, self.last)
            return self.last
        return self.generator(table, None)


@dataclass(slots=True, frozen=True)
class TableColumn[T]:
    name: str
    typename: str
    default: T | None = None
    generator: TableColumnValueGenerator | None = None
    primary: bool = False
    unique: bool = False
    nullable: bool = True

    @classmethod
    def from_sql(cls, sql: str, default: T | None = None) -> Self:
        name, typename = sql.split(' ', 2)[:2]
        if isinstance(default, str) and default.startswith('$TableColumnValueGenerator:'):
            generator = TableColumnValueGenerator.instances.get(generator_name := default.split(':', 1)[1])
            if generator is None:
                raise ValueError(f'No generator registered for {generator_name}!')
        else:
            generator = None
        return cls(name, typename, default, generator, 'PRIMARY KEY' in sql, 'UNIQUE' in sql, 'NOT NULL' not in sql)

    @property
    def sql(self) -> str:
        parser = Parser.get_by_typename(self.typename) or BuiltinParser.TEXT
        if self.generator and self.generator.name:
            default = '$TableColumnValueGenerator:' + self.generator.name
        elif self.default is not None:
            default = parser.dumps(self.default)
        else:
            default = None
        return (
            self.name + ' ' + self.typename +
            (' NOT NULL' if not self.nullable else '') +
            (' PRIMARY KEY' if self.primary else '') +
            (' UNIQUE' if self.unique else '') +
            ((' DEFAULT ' + f'`{default}`' if isinstance(default, str) else default) if default is not None else '')
        )

    async def get_default(self, table: 'Table[T]') -> T | None:
        return await self.generator.next_value(table) if self.generator else self.default


@dataclass(slots=True, frozen=True)
class Table[T](ABC):
    name: str
    columns: Tuple[TableColumn[T], ...]
    record: Type[TableRecord[T]] = field(init=False)

    def __post_init__(self) -> None:
        assert self.columns, 'Table must have at least one column!'
        object.__setattr__(
            self,
            'record',
            TableRecord.make_subclass(self, *self.columns)
        )

    @abstractmethod
    async def contains(self, record: TableRecord[T]) -> bool: ...

    @abstractmethod
    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> Iterator[TableRecord[T]]: ...

    async def select_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        return next(await self.select(predicate), None)

    @abstractmethod
    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None: ...

    @abstractmethod
    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def delete_one(self, predicate: TableSelectionPredicate[T] | None = None) -> None: ...

    @abstractmethod
    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None: ...

    @abstractmethod
    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None: ...

    async def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> Iterator[TableRecord[T]]:
        selected = await self.select(predicate)
        await self.delete(predicate)
        return selected

    async def pop_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        selected = await self.select_one(predicate)
        await self.delete_one(predicate)
        return selected


@dataclass(slots=True, frozen=True)
class MemoryTable[T](Table[T]):
    _records: Set[TableRecord[T]] = field(default_factory=set)

    async def contains(self, record: TableRecord[T]) -> bool:
        return record in self._records

    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> Iterator[TableRecord[T]]:
        return (record for record in self._records if predicate(record, self)) if predicate else iter(self._records)

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = await self.record.make(**values)
        if record in self._records:
            if ignore_on_repeat:
                return
            self._records.discard(record)
            record = await record.replace(**values)
        self._records.add(record)

    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            self._records.clear()
            return
        for record in self._records.copy():
            if predicate(record, self):
                self._records.discard(record)

    async def delete_one(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        if record := await self.select_one(predicate):
            self._records.discard(record)

    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        if not self._records:
            return
        elif not predicate:
            object.__setattr__(
                self,
                '_records',
                {await record.replace(**to_update) for record in self._records}
            )
            return
        for record in self._records.copy():
            if predicate(record, self):
                self._records.discard(record)
                self._records.add(await record.replace(**to_update))

    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        if record := await self.select_one(predicate):
            self._records.discard(record)
            self._records.add(await record.replace(**to_update))


@dataclass(slots=True, frozen=True)
class SQLTable[T](Table[T], ABC):
    database: str
    _executor_factory: _ExecutorFactory = field(default=single_executor)
    _default_selector: str = field(init=False)

    @classmethod
    async def from_database(cls, name: str, database: str, executor_factory: _ExecutorFactory = single_executor) -> Self:
        columns, generated_columns = [], []
        async for parameters in azip(columns_sql(database, name), columns_defaults(database, name)):
            columns.append(TableColumn.from_sql(*parameters))
            if columns[-1].generator:
                generated_columns.append(columns[-1])
        if generated_columns:
            last_record = (await single_executor(database)('SELECT %s FROM %s ORDER BY rowid DESC LIMIT 1' % (
                ', '.join(generated_column.name for generated_column in generated_columns),
                name
            ))).fetchone()
            if last_record is not None:
                for generated_column, last in zip(generated_columns, last_record):
                    generated_column.generator.last = last
        return cls(name, tuple(columns), database, executor_factory)

    def __post_init__(self) -> None:
        Table.__post_init__(self)
        object.__setattr__(self, 'database', abspath(self.database))
        object.__setattr__(self, '_default_selector', 'WHERE ' + ' AND '.join(f'{k} = ?' for k in self.record.fields))

    @property
    def _executor(self) -> Executor:
        return self._executor_factory(self.database)

    async def _execute_where(self, record: TableRecord[T], query: str, parameters: Iterable[Any] = (), **conn_kwargs: Any) -> Cursor | None:
        if self.record.nonrepeating:
            key = self.record.nonrepeating[0]
            return await self._executor(query + f' WHERE {key} = ?', (*parameters, getattr(record, key)), **conn_kwargs)
        elif None in record:
            values = {key: value for key in self.record.fields if (value := getattr(record, key)) is not None}
            return await self._executor(query + ' WHERE ' + ' AND '.join(f'{key} = ?' for key in values), (*parameters, *values.values()), **conn_kwargs)
        return await self._executor(query + ' ' + self._default_selector, (*parameters, *record), **conn_kwargs)

    async def run_executor(self) -> None:
        return await Executor.instances[self.database].run()

    @abstractmethod
    async def create(self) -> None: ...

    @abstractmethod
    async def drop(self) -> None: ...


@dataclass(slots=True, frozen=True)
class SolidTable[T](SQLTable[T]):
    async def create(self) -> None:
        await self.drop()
        await self._executor(f'CREATE TABLE {self.name} ({", ".join(column.sql for column in self.columns)})')

    async def drop(self) -> None:
        await self._executor(f'DROP TABLE IF EXISTS {self.name}')

    async def contains(self, record: TableRecord[T]) -> bool:
        cursor = await self._execute_where(record, f'SELECT * FROM {self.name}')
        return bool(cursor.fetchone()) if cursor else False

    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> Iterator[TableRecord[T]]:
        cursor = await self._executor(f'SELECT * FROM {self.name}')
        return iter(
            [record for values in cursor if predicate(record := await self.record.make(*values), self)]
            if predicate else
            [await self.record.make(*values) for values in cursor]
        ) if cursor else iter(())

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = await self.record.make(**values)
        await self._executor(
            'INSERT OR %s INTO %s VALUES (%s)' % (
                'IGNORE' if ignore_on_repeat else 'REPLACE',
                self.name,
                ', '.join('?' * len(record))
            ),
            record
        )

    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        if not predicate:
            await self._executor(f'DELETE FROM {self.name}')
            return
        for record in await self.select(predicate):
            await self._execute_where(record, f'DELETE FROM {self.name}')

    async def delete_one(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        if record := await self.select_one(predicate):
            await self._execute_where(record, f'DELETE FROM {self.name}')

    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        sql = 'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update))
        if not predicate:
            await self._executor(sql, to_update.values())
            return
        for record in await self.select(predicate):
            await self._execute_where(record, sql, to_update.values())

    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        if record := await self.select_one(predicate):
            await self._execute_where(
                record,
                'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update)),
                to_update.values()
            )


@dataclass(slots=True, frozen=True)
class MemoizedTable[T](SQLTable[T]):
    _records: Set[TableRecord[T]] = field(default_factory=set)

    @classmethod
    async def from_database(cls, name: str, database: str, executor_factory: _ExecutorFactory = deferred_executor) -> Self:
        table = await super(MemoizedTable, cls).from_database(name, database, executor_factory)
        if cursor := await single_executor(database)(f'SELECT * FROM {name}'):
            object.__setattr__(table, '_records', {await table.record.make(*values) for values in cursor})
        return table

    async def create(self) -> None:
        await self.drop()
        await self._executor(f'CREATE TABLE {self.name} ({", ".join(column.sql for column in self.columns)})')
        records = self._records.copy()
        self._records.clear()
        for record in records:
            await self.insert(**record.asdict())

    async def drop(self) -> None:
        await self._executor(f'DROP TABLE IF EXISTS {self.name}')

    async def contains(self, record: TableRecord[T]) -> bool:
        return record in self._records

    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> Iterator[TableRecord[T]]:
        return (record for record in self._records if predicate(record, self)) if predicate else iter(self._records)

    async def insert(self, ignore_on_repeat: bool = False, **values: T) -> None:
        record = await self.record.make(**values)
        if record in self._records:
            if ignore_on_repeat:
                return
            self._records.discard(record)
            record = await record.replace(**values)
        self._records.add(record)
        await self._executor(f'INSERT INTO {self.name} VALUES ({", ".join("?" * len(record))})', record)

    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        if not self._records:
            return
        elif not predicate:
            self._records.clear()
            await self._executor(f'DELETE FROM {self.name}')
            return
        requests = []
        for record in self._records.copy():
            if predicate(record, self):
                self._records.discard(record)
                requests.append(self._execute_where(record, f'DELETE FROM {self.name}'))
        await gather(*requests)

    async def delete_one(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        if record := await self.select_one(predicate):
            self._records.discard(record)
            await self._execute_where(record, f'DELETE FROM {self.name}')

    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        if not self._records:
            return
        sql = 'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update))
        if not predicate:
            object.__setattr__(self, '_records', {await record.replace(**to_update) for record in self._records})
            await self._executor(sql, to_update.values())
            return
        requests = []
        for record in self._records.copy():
            if predicate(record, self):
                self._records.discard(record)
                self._records.add(await record.replace(**to_update))
                requests.append(self._execute_where(record, sql, to_update.values()))
        await gather(*requests)

    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        if record := await self.select_one(predicate):
            self._records.discard(record)
            self._records.add(await record.replace(**to_update))
            await self._execute_where(
                record,
                'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update)),
                to_update.values()
            )
