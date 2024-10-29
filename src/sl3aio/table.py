from collections.abc import AsyncIterator, Callable, Awaitable, Container, Generator
from dataclasses import dataclass, field, replace
from re import DOTALL, IGNORECASE, MULTILINE, match as re_match, search
from typing import Any, ClassVar, Self, Protocol, Never
from inspect import iscoroutinefunction
from abc import ABC, abstractmethod
from .executor import *
from .dataparser import Parser, BuiltinParser

__all__ = [
    'TableRecord', 'TableSelectionPredicate', 'TableColumnValueGenerator',
    'TableColumn', 'Table', 'MemoryTable', 'SqlTable', 'SolidTable'
]


class TableRecord[T](tuple[T, ...]):
    __slots__: ClassVar[tuple[()]] = ()
    table: ClassVar['Table'] = Never
    nonrepeating: ClassVar[tuple[str, ...]] = Never
    fields: ClassVar[tuple[str, ...]] = Never

    @classmethod
    def make_subclass(cls, table: 'Table[T]', *columns: 'TableColumn[T]') -> type[Self]:
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

    def __new__(cls, *args: T, **kwargs: T) -> Self:
        params = dict(zip(cls.fields, args)) | kwargs
        result = []
        for column in cls.table.columns:
            value = params[column.name] if column.name in params else column.get_default(cls.table)
            if value is None and not column.nullable:
                raise ValueError(f'{column.name} cannot be None!')
            result.append(value)
        return super().__new__(cls, result)

    def asdict(self) -> dict[str, T]:
        return dict(zip(self.fields, self))

    def astuple(self) -> tuple[T, ...]:
        return tuple(self)

    async def replace(self, **to_replace: T) -> Self:
        return await self.table._executor(self.__class__, **(self.asdict() | to_replace))

    def __getattr__(self, name: str) -> T:
        return self[type(self).fields.index(name)]
    
    def __getitem__(self, key: int | slice | str) -> T:
        return super().__getitem__(key) if isinstance(key, (int, slice)) else self[type(self).fields.index(key)]
    
    def __eq__(self, other: Self) -> bool:
        return any(getattr(self, k) == getattr(other, k) for k in self.nonrepeating)

    def __hash__(self) -> int:
        return hash(tuple(getattr(self, k) for k in self.nonrepeating))


class TableSelectionPredicate[T](Protocol):
    def __call__(self, record: TableRecord[T]) -> bool: ...


@dataclass(slots=True)
class TableColumnValueGenerator[T]:
    instances: ClassVar[dict[str, Self]] = {}
    generator: Callable[['Table[T]', T | None], T | Awaitable[T]]
    uses_last: bool = False
    last: T | None = None
    name: str | None = None

    @classmethod
    def get(cls, name: str) -> Self:
        return cls.instances[name]

    @property
    def is_async(self) -> bool:
        return iscoroutinefunction(self.generator)

    def register(self, name: str | None = None) -> Self:
        self.name = name or self.name
        self.instances[self.name] = self
        return self

    def next_value(self, table: 'Table[T]') -> T:
        result = self.generator(table, self.last)
        if self.is_async:
            result = table._executor._loop.run_until_complete(result)
        if self.uses_last:
            self.last = result
        return result


@dataclass(slots=True, frozen=True)
class TableColumn[T]:
    name: str
    typename: str
    default: T | None = None
    generator: TableColumnValueGenerator[T] | None = None
    primary: bool = False
    unique: bool = False
    nullable: bool = True

    @classmethod
    def from_sql(cls, sql: str, default: T | None = None) -> Self:
        name, typename = sql.split(' ', 2)[:2]
        try:
            generator = TableColumnValueGenerator.get(default)
        except KeyError:
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

    def get_default(self, table: 'Table[T]') -> T | None:
        return self.generator.next_value(table) if self.generator else self.default


@dataclass(slots=True)
class Table[T](ABC):
    name: str
    _columns: tuple[TableColumn[T], ...]
    _record: type[TableRecord[T]] = field(init=False)
    _executor: ConsistentExecutor = field(init=False, default_factory=ConsistentExecutor)

    def __post_init__(self) -> None:
        assert self._columns, 'Table must have at least one column!'
        self._record = TableRecord.make_subclass(self, *self._columns)

    @property
    def columns(self) -> tuple[TableColumn[T], ...]:
        return self._columns
    
    async def make_record(self, *args: T, **kwargs: T) -> TableRecord[T]:
        return await self._executor(self._record, *args, **kwargs)
    
    async def start_executor(self) -> None:
        await self._executor.start()
    
    async def stop_executor(self) -> None:
        await self._executor.stop()

    @abstractmethod
    async def contains(self, record: TableRecord[T]) -> bool: ...

    @abstractmethod
    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]: ...

    async def insert_many(self, ignore_existing: bool = False, *values: dict[str, T]) -> AsyncIterator[TableRecord[T]]:
        for _values in values:
            yield await self.insert(ignore_existing, **_values)

    @abstractmethod
    def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]: ...

    async def select_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        return await anext(self.select(predicate), None)
    
    @abstractmethod
    def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]: ...

    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        async for _ in self.pop(predicate):
            pass

    async def delete_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        return await anext(self.pop(predicate), None)
    
    @abstractmethod
    def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]: ...

    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        async for _ in self.updated(predicate, **to_update):
            pass

    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> TableRecord[T] | None:
        return await anext(self.updated(predicate, **to_update), None)
    
    async def __aenter__(self) -> Self:
        await self._executor.__aenter__()
        return self
    
    async def __aexit__(self, *args) -> None:
        await self._executor.__aexit__(*args)
    

@dataclass(slots=True)
class MemoryTable[T](Table[T]):
    _records: set[TableRecord[T]] = field(default_factory=set)

    async def contains(self, record: TableRecord[T]) -> bool:
        return await self._executor(set.__contains__, self._records, record)
    
    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        record = await self._executor(self._record, **values)
        if await self.contains(record) and not ignore_existing:
            await self._executor(set.discard, self._records, record)
        await self._executor(set.add, self._records, record)
        return record
    
    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if predicate is None:
            for record in self._records.copy():
                yield record
        else:
            p_executor = Executor()
            for record in self._records.copy():
                if await p_executor(predicate, record):
                    yield record
    
    async def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if not predicate:
            for record in self._records.copy():
                await self._executor(set.discard, self._records)
                yield record
        else:
            p_executor = Executor()
            for record in self._records.copy():
                if await p_executor(predicate, record):
                    await self._executor(set.discard, self._records, record)
                    yield record
    
    async def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        if not predicate:
            for record in self._records.copy():
                new_record = await record.replace(**to_update)
                await self._executor(set.discard, self._records, record)
                await self._executor(set.add, self._records, new_record)
                yield new_record
        else:
            for record in self._records.copy():
                p_executor = Executor()
                if await p_executor(predicate, record):
                    new_record = await record.replace(**to_update)
                    await self._executor(set.discard, self._records, record)
                    await self._executor(set.add, self._records, new_record)
                    yield new_record


@dataclass(slots=True)
class SqlTable[T](Table[T], ABC):
    _executor: ConnectionManager
    _default_selector: str = field(init=False)

    @classmethod
    async def from_database(cls, name: str, executor: ConnectionManager) -> Self:        
        columns, generated_columns = [], []
        table_sql = await (await executor.execute(f'SELECT sql FROM sqlite_master WHERE type = "table" AND name = "{name}"')).fetchone()
        columns_data = await executor.execute(f'SELECT type, dflt_value FROM pragma_table_info("{name}")')

        for column_sql in re_match(r'^CREATE TABLE\s+\w+\s*\((.*)\)$', table_sql[0], IGNORECASE).group(1).split(', '):
            typename, default = await anext(columns_data)
            if isinstance(default, str) and (match_ := search(r'["\'`](.*)["\'`]', default, DOTALL | MULTILINE)):
                default = match_.group(1)
            try:
                default = Parser.get_by_typename(typename).loads(default)
            except (TypeError, ValueError, AttributeError):
                pass
            columns.append(TableColumn.from_sql(column_sql, default))
            if columns[-1].generator:
                generated_columns.append(columns[-1])
            
        if generated_columns:
            last_record = await (await executor.execute('SELECT %s FROM %s ORDER BY rowid DESC LIMIT 1' % (
                ', '.join(
                    generated_column.name
                    for generated_column in generated_columns
                    if generated_column.generator.uses_last
                ),
                name
            ))).fetchone()
            if last_record is not None:
                for generated_column, last in zip(generated_columns, last_record):
                    generated_column.generator.last = last
        return cls(name, tuple(columns), executor)

    def __post_init__(self) -> None:
        super(SqlTable, self).__post_init__()
        self._default_selector = 'WHERE ' + ' AND '.join(f'{k} = ?' for k in self._record.fields)

    @property
    def database(self) -> str:
        return self._executor.database
    
    @abstractmethod
    async def create(self) -> None: ...

    @abstractmethod
    async def drop(self) -> None: ...

    async def _execute_where(self, query: str, record: TableRecord[T], parameters: Parameters = ()) -> CursorManager:
        if self._record.nonrepeating:
            key = record.nonrepeating[0]
            return await self._executor.execute(f'{query} WHERE {key} = ?', (*parameters, getattr(record, key)))
        elif None in record:
            values = await self._executor(dict, ((k, v) for k in record.fields if (v := getattr(record, k)) is not None))
            return await self._executor.execute(
                f'{query} WHERE ' + self._executor(' AND '.join, (f'{k} = ?' for k in values)),
                (*parameters, *values.values())
            )
        return await self._executor.execute(f'{query} {self._default_selector}', (*parameters, *record))


@dataclass(slots=True)
class SolidTable[T](SqlTable[T]):
    async def contains(self, record: TableRecord[T]) -> bool:
        return await (await self._execute_where(f'SELECT * FROM {self.name}', record)).fetchone() is not None

    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        record = await self._executor(self._record, **values)
        await self._executor.execute(
            'INSERT OR %s INTO %s VALUES (%s)' % ('IGNORE' if ignore_existing else 'REPLACE', self.name, ', '.join('?' * len(record))),
            record
        )
        return record
    
    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if not predicate:
            async for record_data in await self._executor.execute(f'SELECT * FROM {self.name}'):
                yield await self._executor(self._record, *record_data)
        else:
            p_executor = Executor()
            async for record_data in await self._executor.execute(f'SELECT * FROM {self.name}'):
                if await p_executor(predicate, record := await p_executor(self._record, *record_data)):
                    yield record
    
    async def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if not predicate:
            async for record_data in await self._executor.execute(f'DELETE FROM {self.name} RETURNING *'):
                yield await self._executor(self._record, *record_data)
        else:
            p_executor = Executor()
            async for record_data in await self._executor.execute(f'SELECT * FROM {self.name}'):
                if await p_executor(predicate, record := await p_executor(self._record, *record_data)):
                    await self._execute_where(f'DELETE FROM {self.name}', record)
                    yield record
    
    async def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        sql = 'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update))
        if not predicate:
            async for record_data in await self._executor.execute(f'{sql} RETURNING *', to_update.values()):
                yield await self._executor(self._record, *record_data)
        else:
            p_executor = Executor()
            async for record_data in await self._executor.execute(f'SELECT * FROM {self.name}'):
                if await p_executor(predicate, record := await p_executor(self._record, *record_data)):
                    await self._execute_where(sql, record, to_update.values())
                    yield record

    async def create(self) -> None:
        await self.drop()
        await self._executor.execute(f'CREATE TABLE {self.name} ({", ".join(column.sql for column in self.columns)})')
    
    async def drop(self) -> None:
        await self._executor.execute(f'DROP TABLE IF EXISTS {self.name}')
