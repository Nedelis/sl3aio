"""
This module provides a set of classes for working with database tables in an object-oriented manner.

The module includes classes for representing table records, columns, and different types of tables
(memory-based and SQL-based). It also provides utility classes for generating column values and
handling table selection predicates.

Classes
-------
1. TableRecord: Represents a single record in a table.
2. TableSelectionPredicate: Protocol for defining predicates used in table selection operations.
3. TableColumnValueGenerator: Generates values for table columns.
4. TableColumn: Represents a column in a table.
5. Table: Abstract base class for all table types.
6. MemoryTable: In-memory implementation of a table.
7. SqlTable: Abstract base class for SQL-based tables.
8. SolidTable: Concrete implementation of an SQL-based table.

This module is designed to work with asynchronous operations and provides a flexible and
extensible framework for database operations.
"""
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from re import DOTALL, IGNORECASE, MULTILINE, match as re_match, compile, Pattern
from typing import ClassVar, Self, Protocol
from abc import ABC, abstractmethod
from asyncio import get_event_loop
from inspect import isawaitable
from .executor import *
from .dataparser import Parser, BuiltinParsers

__all__ = [
    'TableRecord', 'TableSelectionPredicate', 'TableColumnValueGenerator',
    'TableColumn', 'Table', 'MemoryTable', 'SqlTable', 'SolidTable'
]

_ColumnsSqlFromTable: Pattern[str] = compile(r'^CREATE TABLE\s+\w+\s*\((.*)\)$', IGNORECASE)
_RemoveQuotation: Pattern[str] = compile(r'^["\'`](.*)["\'`]$', DOTALL | MULTILINE)


class TableRecord[T](tuple[T, ...]):
    """Represents a single record in a table.

    This class extends the built-in tuple class to provide additional functionality
    specific to table records.
    """
    __slots__ = ()
    table: ClassVar['Table']
    """The table to which this record belongs."""
    nonrepeating: ClassVar[tuple[str, ...]]
    """Names of columns that are unique or primary keys."""
    fields: ClassVar[tuple[str, ...]]
    """Names of all columns in the table."""
    executor: ClassVar[Executor]
    """An executor for running operations on the record."""

    @classmethod
    def make_subclass[T](cls, table: 'Table[T]', *columns: 'TableColumn[T]') -> type['TableRecord[T]']:
        r"""Create a new subclass of TableRecord for a specific table.

        Parameters
        ----------
        table : :class:`Table` [`T`]
            The table for which to create the subclass.
        \*columns : :class:`TableColumn` [`T`]
            The columns of the table.

        Returns
        -------
        `type` [:class:`TableRecord` [`T`]]
            A new subclass of TableRecord.
        """
        fields, nonrepeating = [], []
        for column in columns:
            fields.append(column.name)
            if column.unique or column.primary:
                nonrepeating.append(column.name)
        return type((table.name.title() if table.name else '') + 'TableRecord', (cls,), {
            'table': table,
            'nonrepeating': tuple(nonrepeating),
            'fields': tuple(fields),
            'executor': Executor()
        })
    
    @classmethod
    async def make(cls, *args: T, **kwargs: T) -> Self:
        r"""Asynchronously create a new instance of the record.

        The positional arguments must be in the order of the columns in the table.
        The names of the keyword arguments must match the names of the columns in the table.
        If a value is not passed for some columns, value, obtained from :meth:`TableColumn.get_default`,
        will be used.

        Parameters
        ----------
        \*args : `T`
            Positional arguments for the record values.
        \*\*kwargs : `T`
            Keyword arguments for the record values.

        Returns
        -------
        :class:`TableRecord` [`T`]
            A new instance of the record.
        
        .. note::
            You must provide values for every column in the table that can't be null and doesn't
            have a default value. 
        """
        return await cls.executor(cls, *args, **kwargs)

    def __new__(cls, *args: T, **kwargs: T) -> Self:
        """Create a new instance of the record. Same as :meth:`TableRecord.make` but sync."""
        params = dict(zip(cls.fields, args)) | kwargs
        return super().__new__(
            cls,
            (params[column.name] if column.name in params else column.get_default() for column in cls.table.columns)
        )

    def asdict(self) -> dict[str, T]:
        """Convert the record to a dictionary.

        Returns
        -------
        `dict` [`str`, `T`]
            A dictionary representation of the record.
        """
        return dict(zip(self.fields, self))

    def astuple(self) -> tuple[T, ...]:
        """Convert the record to a tuple.

        Returns
        -------
        `tuple` [`T`, `...`]
            A tuple representation of the record.
        """
        return tuple(self)

    async def replace(self, **to_replace: T) -> Self:
        r"""Create a new record with some values replaced.

        Parameters
        ----------
        \*\*to_replace : `T`
            The values to replace in the new record.

        Returns
        -------
        :class:`TableRecord` [`T`]
            A new record with the specified values replaced.
        """
        return await self.make(**(self.asdict() | to_replace))

    def __getattr__(self, name: str) -> T:
        return self[type(self).fields.index(name)]
    
    def __getitem__(self, key: int | slice | str) -> T:
        return super().__getitem__(key) if isinstance(key, (int, slice)) else self[type(self).fields.index(key)]
    
    def __eq__(self, other: Self) -> bool:
        return any(getattr(self, k) == getattr(other, k) for k in self.nonrepeating)

    def __hash__(self) -> int:
        return hash(tuple(getattr(self, k) for k in self.nonrepeating))


class TableSelectionPredicate[T](Protocol):
    """Protocol for defining predicates used in table selection operations.

    This protocol defines the interface for callable objects that can be used
    to filter records in table operations.
    """

    async def __call__(self, record: TableRecord[T]) -> bool:
        """Evaluate the predicate for a given record.

        Parameters
        ----------
        record : :class:`TableRecord` [`T`]
            The record to evaluate on.

        Returns
        -------
        `bool`
            True if the record matches the predicate, False otherwise.
        """


@dataclass(slots=True)
class TableColumnValueGenerator[T]:
    """Generates values for table columns.

    This class is used to create generators for column values, which can be
    used as default values or for generating values during insert operations.
    """
    _instances: ClassVar[dict[str, Self]] = {}
    name: str
    """The name of the generator."""
    generator: Callable[[], T | Awaitable[T]]
    """The function that generates values for the column. Can be both async and sync."""
    
    @classmethod
    def make[T](
        cls,
        name: str,
        generator: Callable[[], T | Awaitable[T]],
        register: bool = True
    ) -> 'TableColumnValueGenerator[T]':
        """Create a new TableColumnValueGenerator instance.

        Parameters
        ----------
        name : `str`
            The name of the generator.
        generator : `Callable` [[], `T` | `Awaitable` [`T`]]
            The function that generates values.
        register : `bool`, optional
            Whether to register the generator globally. Defaults to True.

        Returns
        -------
        :class:`TableColumnValueGenerator` [`T`]
            A new instance of TableColumnValueGenerator.
        """
        return cls(name, generator).register() if register else cls(name, generator)

    @classmethod
    def from_function[T](
        cls,
        name: str,
        register: bool = True
    ) -> Callable[[Callable[[], T | Awaitable[T]]], 'TableColumnValueGenerator[T]']:
        """Decorator to create a TableColumnValueGenerator from a function.

        Parameters
        ----------
        name : `str`
            The name of the generator.
        register : `bool`, optional
            Whether to register the generator globally. Defaults to True.

        Returns
        -------
        callable
            A decorator that creates a TableColumnValueGenerator.
        """
        def decorator(func: Callable[[], T | Awaitable[T]]) -> 'TableColumnValueGenerator[T]':
            nonlocal name
            return cls.make(name, func, register)
        return decorator

    @classmethod
    def get_by_name(cls, name: str) -> Self | None:
        """Retrieve a registered generator by name.

        Parameters
        ----------
        name : `str`
            The name of the generator to retrieve.

        Returns
        -------
        :class:`TableColumnValueGenerator` | `None`
            The registered generator, or None if not found.
        """
        return cls._instances[name].copy() if name in cls._instances else None
    
    def copy(self) -> Self:
        """Create a copy of the generator.

        Returns
        -------
        :class:`TableColumnValueGenerator` [`T`]
            A new instance of the generator with the same attributes.
        """
        return type(self)(name=self.name, generator=self.generator)
    
    def register(self) -> Self:
        """Register the copy of generator for global use.

        Returns
        -------
        :class:`TableColumnValueGenerator` [`T`]
            Self for chaining.
        """
        self._instances[self.name] = self.copy()
        return self
    
    def unregister(self) -> Self:
        """Unregister the generator.

        Returns
        -------
        :class:`TableColumnValueGenerator` [`T`]
            Self for chaining.
        """
        self._instances.pop(self.name, None)
        return self
    
    def __next__(self) -> T:
        if isawaitable(result := self.generator()):
            return get_event_loop().run_until_complete(result)
        return result


@dataclass(slots=True, frozen=True)
class TableColumn[T]:
    """Represents a column in a table.

    This class defines the properties of a table column, including its name,
    data type, and constraints.

    .. note::
        If both the ``generator`` and the ``default`` parameters are specified,
        preference is given to ``generator``.

    See Also
    --------
    - :class:`TableColumnValueGenerator`
    """
    name: str
    """The name of the column."""
    typename: str
    """The data type of the column."""
    default: T | None = None
    """The default value for the column. Defaults to `None`."""
    generator: TableColumnValueGenerator[T] | None = None
    """A generator for column values. Defaults to `None`."""
    primary: bool = False
    """Whether this column is a primary key. Defaults to `False`."""
    unique: bool = False
    """Whether this column has a unique constraint. Defaults to `False`."""
    nullable: bool = True
    """Whether this column can contain NULL values. Defaults to `True`."""

    @classmethod
    def from_sql[T](cls, sql: str, default: T | None = None) -> 'TableColumn[T]':
        """Create a TableColumn instance from an SQL column definition.

        Parameters
        ----------
        sql : `str`
            The SQL definition of the column.
        default : `T` | `None`, optional
            The default value for the column.

        Returns
        -------
        :class:`TableColumn` [`T`]
            A new TableColumn instance.

        .. note::
            Columns, that generated by sqlite using ``GENERATED`` keyword, won't be
            interpreted correctly. To make column generated, you need to create generator
            for it using the :class:`TableColumnValueGenerator` and then set column ``DEFAULT``
            value to ``"$Generated:generator_name"``
        """
        name, typename = sql.split(' ', 2)[:2]
        if isinstance(default, str) and default.startswith('$Generated:'):
            generator = TableColumnValueGenerator.get_by_name(default[11:])
        else:
            generator = None
        return cls(name, typename, default, generator, 'PRIMARY KEY' in sql, 'UNIQUE' in sql, 'NOT NULL' not in sql)

    def to_sql(self) -> str:
        """Generate the SQL representation of the column.

        Returns
        -------
        `str`
            The SQL definition of the column.
        
        .. note::
            If column's typename is not present in :class:`sl3aio.parser.Parser` registry,
            it would be represented as ``TEXT``.

        .. note::
            Columns with specified generator would have their ``DEFAULT`` value setted
            to ``"$Generated:generator_name"``.
        """
        parser = Parser.get_by_typename(self.typename) or BuiltinParsers.TEXT
        if self.generator:
            default = f'$Generated:{self.generator.name}'
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

    def get_default(self) -> T | None:
        """Get the default value for the column.

        First this method will try to get the default value from generator. If it not
        specified it will use the default value.

        Returns
        -------
        `T` | `None`
            The default value for the column.
        """
        return next(self.generator) if self.generator else self.default


@dataclass(slots=True)
class Table[T](ABC):
    """Abstract base class for all table types.

    This class defines the common interface and functionality for different
    types of tables (e.g., in-memory tables, SQL tables).
    """
    name: str
    """The name of the table."""
    _columns: tuple[TableColumn[T], ...]
    """The column of the table. This field is protected, use :obj:`Table.columns` instead."""
    _record_type: type[TableRecord[T]] = field(init=False)
    """The type used for records in this table."""
    _executor: ConsistentExecutor = field(init=False, default_factory=ConsistentExecutor)
    """An executor for running operations on the table."""

    def __post_init__(self) -> None:
        assert self._columns, 'Table must have at least one column!'
        self._record_type = TableRecord.make_subclass(self, *self._columns)

    @property
    def columns(self) -> tuple[TableColumn[T], ...]:
        """The columns of the table."""
        return self._columns

    async def make_record(self, *args: T, **kwargs: T) -> TableRecord[T]:
        r"""Create a new record for this table. For more details, see :meth:`TableRecord.make`.

        Parameters
        ----------
        \*args : `T`
            Positional arguments for the record values.
        \*\*kwargs : `T`
            Keyword arguments for the record values.

        Returns
        -------
        :class:`TableRecord` [`T`]
            A new record for this table.
        """
        return await self._record_type.make(*args, **kwargs)
    
    async def start_executor(self) -> None:
        """Start the table's executor."""
        await self._executor.start()
    
    async def stop_executor(self) -> None:
        """Stop the table's executor."""
        await self._executor.stop()

    @abstractmethod
    async def length(self) -> int:
        """Get the number of records in the table.

        Returns
        -------
        `int`
            The number of records in the table.
        """

    @abstractmethod
    async def contains(self, record: TableRecord[T]) -> bool:
        """Check if a record exists in the table.

        Parameters
        ----------
        record : :class:`TableRecord` [`T`]
            The record to check for.

        Returns
        -------
        `bool`
            True if the record exists in the table, False otherwise.
        """

    @abstractmethod
    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        r"""Insert a new record into the table.

        Parameters
        ----------
        ignore_existing : `bool`, optional
            Whether to ignore existing records. Defaults to False.
        \*\*values : `T`
            The values for the new record.

        Returns
        -------
        :class:`TableRecord` [`T`]
            The inserted record.

        See Also
        --------
        - :meth:`Table.insert_many`
        """

    async def insert_many(self, ignore_existing: bool = False, *values: dict[str, T]) -> AsyncIterator[TableRecord[T]]:
        r"""Insert multiple records into the table.

        Parameters
        ----------
        ignore_existing : `bool`, optional
            Whether to ignore existing records. Defaults to False.
        \*values : `dict` [`str`, `T`]
            Dictionaries containing the values for each record to insert.

        Yields
        ------
        :class:`TableRecord` [`T`]
            The inserted records.

        See Also
        --------
        - :meth:`Table.insert`
        """
        for record_values in values:
            yield await self.insert(ignore_existing, **record_values)

    @abstractmethod
    def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Select records from the table. If predicate isn't specified, yields the whole table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records. Defaults to None.

        Yields
        ------
        :class:`TableRecord` [`T`]
            The selected records.

        See Also
        --------
        - :meth:`Table.select_one`
        """

    async def select_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        """Select a single record from the table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records. Defaults to None.

        Returns
        -------
        :class:`TableRecord` [`T`] | `None`
            The selected record, or None if no record matches.

        See Also
        --------
        - :meth:`Table.select`
        """
        return await anext(self.select(predicate), None)
    
    @abstractmethod
    def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Remove and return records from the table. If predicate isn't specified, pops the whole table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records to remove. Defaults to None.

        Yields
        ------
        :class:`TableRecord` [`T`]
            The removed records.

        See Also
        --------
        - :meth:`Table.delete`
        - :meth:`Table.delete_one`
        """

    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        """Delete records from the table. If predicate isn't specified, clears the table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records to delete. Defaults to None.

        See Also
        --------
        - :meth:`Table.pop`
        - :meth:`Table.delete_one`
        """
        async for _ in self.pop(predicate):
            pass

    async def delete_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        """Delete a single record from the table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the record to delete. Defaults to None.

        Returns
        -------
        :class:`TableRecord` [`T`] | `None`
            The deleted record, or None if no record matches.

        See Also
        --------
        - :meth:`Table.delete`
        - :meth:`Table.delete_one`
        """
        return await anext(self.pop(predicate), None)
    
    @abstractmethod
    def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        r"""Update records in the table and yield the updated records. If predicate isn't specified,
        upadates every record.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records to update. Defaults to None.
        \*\*to_update : `T`
            The values to update in the matching records.

        Yields
        ------
        :class:`TableRecord` [`T`]
            The updated records.

        See Also
        --------
        - :meth:`Table.update`
        - :meth:`Table.update_one`
        """

    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        r"""Update records in the table without yielding the updated records.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records to update. Defaults to None.
        \*\*to_update : `T`
            The values to update in the matching records.
        
        See Also
        --------
        - :meth:`Table.updated`
        - :meth:`Table.update_one`
        """
        async for _ in self.updated(predicate, **to_update):
            pass

    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> TableRecord[T] | None:
        r"""Update a single record in the table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the record to update. Defaults to None.
        \*\*to_update : `T`
            The values to update in the matching record.

        Returns
        -------
        :class:`TableRecord` [`T`] | `None`
            The updated record, or None if no record matches.

        See Also
        --------
        - :meth:`Table.updated`
        - :meth:`Table.update`
        """
        return await anext(self.updated(predicate, **to_update), None)
    
    async def __aenter__(self) -> Self:
        """Asynchronous context manager entry point.

        Returns
        -------
        `Self`
            The Table instance.
        """
        await self._executor.__aenter__()
        return self
    
    async def __aexit__(self, *args) -> None:
        r"""Asynchronous context manager exit point.

        Parameters
        ----------
        \*args
            Arguments passed to the exit method.
        """
        await self._executor.__aexit__(*args)
    

@dataclass(slots=True)
class MemoryTable[T](Table[T]):
    _records: set[TableRecord[T]] = field(default_factory=set)

    async def length(self) -> int:
        return len(self._records)

    async def contains(self, record: TableRecord[T]) -> bool:
        return await self._executor(set.__contains__, self._records, record)
    
    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        record = await self._record_type.make(**values)
        if await self.contains(record) and not ignore_existing:
            await self._executor(set.discard, self._records, record)
        await self._executor(set.add, self._records, record)
        return record
    
    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if predicate is None:
            for record in self._records.copy():
                yield record
        else:
            for record in self._records.copy():
                if await predicate(record):
                    yield record
    
    async def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if not predicate:
            for record in self._records.copy():
                await self._executor(set.discard, self._records)
                yield record
        else:
            for record in self._records.copy():
                if predicate(record):
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
                if await predicate(record):
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
        columns: list[TableColumn] = []
        table_sql = (await (await executor.execute(f'SELECT sql FROM sqlite_master WHERE type = "table" AND name = "{name}"')).fetchone())[0]
        columns_data = await executor.execute(f'SELECT type, dflt_value FROM pragma_table_info("{name}")')
        for column_sql in re_match(_ColumnsSqlFromTable, table_sql).group(1).split(', '):
            typename, default = await anext(columns_data)
            if isinstance(default, str) and (match_ := re_match(_RemoveQuotation, default)):
                default = match_.group(1)
            try:
                default = Parser.get_by_typename(typename).loads(default)
            except (TypeError, ValueError, AttributeError):
                pass
            columns.append(TableColumn.from_sql(column_sql, default))
        return cls(name, tuple(columns), executor)

    def __post_init__(self) -> None:
        super(SqlTable, self).__post_init__()
        self._default_selector = 'WHERE ' + ' AND '.join(f'{k} = ?' for k in self._record_type.fields)

    @property
    def database(self) -> str:
        return self._executor.database

    async def _execute_where(self, query: str, record: TableRecord[T], parameters: Parameters = ()) -> CursorManager:
        if self._record_type.nonrepeating:
            key = record.nonrepeating[0]
            return await self._executor.execute(f'{query} WHERE {key} = ?', (*parameters, getattr(record, key)))
        elif None in record:
            values = await record.executor(dict, ((k, v) for k in record.fields if (v := getattr(record, k)) is not None))
            return await self._executor.execute(
                f'{query} WHERE ' + self._executor(' AND '.join, (f'{k} = ?' for k in values)),
                (*parameters, *values.values())
            )
        return await self._executor.execute(f'{query} {self._default_selector}', (*parameters, *record))
    
    @abstractmethod
    async def create(self) -> None: ...

    @abstractmethod
    async def drop(self) -> None: ...


@dataclass(slots=True)
class SolidTable[T](SqlTable[T]):
    async def length(self) -> int:
        return await (await self._executor.execute(f'SELECT MAX(rowid) FROM "{self.name}"')).fetchone()[0]

    async def contains(self, record: TableRecord[T]) -> bool:
        return await (await self._execute_where(f'SELECT * FROM "{self.name}"', record)).fetchone() is not None

    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        record = await self._record_type.make(**values)
        await self._executor.execute(
            'INSERT OR %s INTO %s VALUES (%s)' % ('IGNORE' if ignore_existing else 'REPLACE', self.name, ', '.join('?' * len(record))),
            record
        )
        return record
    
    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if not predicate:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                yield await self._record_type.make(*record_data)
        else:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                if await predicate(record := await self._record_type.make(*record_data)):
                    yield record
    
    async def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        if not predicate:
            async for record_data in await self._executor.execute(f'DELETE FROM "{self.name}" RETURNING *'):
                yield await self._record_type.make(*record_data)
        else:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                if await predicate(record := await self._record_type.make(*record_data)):
                    await self._execute_where(f'DELETE FROM "{self.name}"', record)
                    yield record
    
    async def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        sql = 'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update))
        if not predicate:
            async for record_data in await self._executor.execute(f'{sql} RETURNING *', to_update.values()):
                yield await self._record_type.make(*record_data)
        else:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                if await predicate(record := await self._record_type.make(*record_data)):
                    await self._execute_where(sql, record, to_update.values())
                    yield record

    async def create(self) -> None:
        await self.drop()
        await self._executor.execute(f'CREATE TABLE "{self.name}" ({", ".join(column.to_sql() for column in self.columns)})')
    
    async def drop(self) -> None:
        await self._executor.execute(f'DROP TABLE IF EXISTS "{self.name}"')
