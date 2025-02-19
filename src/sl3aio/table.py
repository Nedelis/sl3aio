"""
Description
-----------
This module provides a set of classes for working with database tables in an object-oriented manner.

The module includes classes for representing table records, columns, and different types of tables
(memory-based and SQL-based). It also provides utility classes for generating column values and
handling table selection predicates and offers an abstraction layer that allows for consistent
interaction with different types of tables.

This module is designed to work with asynchronous operations and provides a flexible and
extensible framework for database operations.

.. Warning::
    Never create table instances outside of an asynchronous context (except when
    you've re-implemented their logic). This is because when creating a table, it
    needs an active event loop. You can use lazy initialization instead:

    .. code-block:: python

        from sl3aio.table import MemoryTable

        class Database:
            my_table: MemoryTable

            @classmethod
            def setup(cls) -> None:
                cls.my_table = MemoryTable("my_table", ...)

        async def main() -> None:
            Database.setup()


Key Components
--------------
- :class:`TableColumn`: Represents a column in a table.
- :class:`TableColumnValueGenerator`: Generates values for table columns.
- :class:`SolidTable`: Concrete implementation of SqlTable for interacting with SQLite databases.
- :class:`MemoryTable`: Implementation of an in-memory table.
- :class:`TableRecord`: Represents a single record in a table.


Other Components
----------------
- :class:`TableSelectionPredicate`: Protocol for defining predicates used in table selection operations.
- :class:`Table`: Abstract base class for all table types.
- :class:`SqlTable`: Abstract base class for SQL-based tables.


Usage Examples
--------------
- Creating and using a :class:`MemoryTable`:

.. code-block:: python

    from sl3aio.table import MemoryTable, TableColumn

    # Define columns
    id_column = TableColumn("id", "INTEGER", primary=True)
    name_column = TableColumn("name", "TEXT", nullable=False)
    age_column = TableColumn("age", "INTEGER", default=0)

    # Create a MemoryTable
    person_table = MemoryTable("persons", (id_column, name_column, age_column))

    # Insert a record
    await person_table.insert(id=1, name="Alice", age=30)

    # Select records
    async for record in person_table.select(lambda r: r.age > 25):
        print(record.name, record.age)

    # Update a record
    await person_table.update(lambda r: r.id == 1, age=31)

    # Delete a record
    await person_table.delete(lambda r: r.name == "Alice")

- Using a :class:`SolidTable` (SQLite):

.. code-block:: python

    from sl3aio.table import SolidTable, TableColumn
    from sl3aio.executor import ConnectionManager

    # Define columns
    id_column = TableColumn("id", "INTEGER", primary=True)
    title_column = TableColumn("title", "TEXT", nullable=False)
    author_column = TableColumn("author", "TEXT", nullable=False)

    # Create a ConnectionManager
    conn_manager = ConnectionManager("path/to/database.db")

    # Create a SolidTable
    book_table = SolidTable("books", (id_column, title_column, author_column), conn_manager)

    # Create the table in the database
    await book_table.create()

    # Insert a record
    await book_table.insert(id=1, title="1984", author="George Orwell")

    # Select records
    async for record in book_table.select(lambda r: r.author == "George Orwell"):
        print(record.title)

    # Update a record
    await book_table.update(lambda r: r.id == 1, title="Nineteen Eighty-Four")

    # Delete a record
    await book_table.delete(lambda r: r.title == "Nineteen Eighty-Four")

    # Drop the table
    await book_table.drop()

- Using :class:`TableColumnValueGenerator`:

.. code-block:: python

    from sl3aio.table import TableColumnValueGenerator, TableColumn, SolidTable
    from random import randint

    # Define a generator for random IDs
    @TableColumnValueGenerator.from_function("random_id")
    def random_id():
        return randint(1000, 9999)

    # Create a column with the generator
    id_column = TableColumn("id", "INTEGER", generator=TableColumnValueGenerator.get_by_name("random_id"))
    name_column = TableColumn("name", "TEXT", nullable=False)

    # Create a table with the generated column
    user_table = SolidTable("users", (id_column, name_column), conn_manager)

    # Insert a record (id will be generated automatically)
    await user_table.insert(name="Bob")

- Using :class:`TableSelectionPredicate`:

.. code-block:: python

    from sl3aio.table import TableSelectionPredicate

    # Define a custom predicate
    class AgeRangePredicate(TableSelectionPredicate):
        def __init__(self, min_age: int, max_age: int):
            self.min_age = min_age
            self.max_age = max_age

        async def __call__(self, record):
            return self.min_age <= record.age <= self.max_age

    # Use the custom predicate
    age_range = AgeRangePredicate(25, 35)
    async for record in person_table.select(age_range):
        print(record.name, record.age)


See Also
--------
:py:mod:`.easytable`: Convinient and easy interface to work with tables.
:py:mod:`.executor`: Core module of this library.
"""
__all__ = [
    'TableRecord', 'TableSelectionPredicate', 'TableColumnValueGenerator',
    'TableColumn', 'Table', 'MemoryTable', 'SqlTable', 'SolidTable'
]

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from re import DOTALL, IGNORECASE, MULTILINE, match as re_match, compile, Pattern
from typing import ClassVar, Self, Protocol
from abc import ABC, abstractmethod
from asyncio import get_event_loop
from inspect import isawaitable
from .executor import *
from .dataparser import Parser, BuiltinParsers

_ColumnsSqlFromTable: Pattern[str] = compile(r'^CREATE TABLE\s+\w+\s*\((.*)\)$', IGNORECASE)
_RemoveQuotation: Pattern[str] = compile(r'^["\'`](.*)["\'`]$', DOTALL | MULTILINE)


class TableRecord[T](tuple[T, ...]):
    """Represents a single record in a table.

    This class extends the built-in tuple class to provide additional functionality
    specific to table records.

    .. Note::
        Hash of the record is same as the hash of the tuple containing only values of unique/primary
        (nonrepeating) columns. The equality operator works similarly. So, as in the sqlite, two records
        of the table, which doesn't have unique/primary columns, are always different.
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

    def __new__[T](cls, *args: T, **kwargs: T) -> 'TableRecord[T]':
        """Create a new instance of the record. Same as :meth:`TableRecord.make` but synchronous."""
        params = dict(zip(cls.fields, args)) | kwargs
        return super().__new__(
            cls,
            (params[column.name] if column.name in params else column.get_default() for column in cls.table.columns)
        )

    @classmethod
    def make_subclass[T](cls, table: 'Table[T]', *columns: 'TableColumn[T]') -> type['TableRecord[T]']:
        """Create a new subclass of TableRecord for a specific table.

        Parameters
        ----------
        table : :class:`Table` [`T`]
            The table for which to create the subclass.
        *columns : :class:`TableColumn` [`T`]
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
    async def make[T](cls, *args: T, **kwargs: T) -> 'TableRecord[T]':
        """Asynchronously create a new instance of the record.

        The positional arguments must be in the order of the columns in the table.
        The names of the keyword arguments must match the names of the columns in the table.
        If a value is not passed for some columns, value, obtained from :meth:`TableColumn.get_default`,
        will be used.
        
        .. Note::
            You must provide values for every column in the table that can't be null and doesn't
            have a default value. 

        Parameters
        ----------
        *args : `T`
            Positional arguments for the record values.
        **kwargs : `T`
            Keyword arguments for the record values.

        Returns
        -------
        :class:`TableRecord` [`T`]
            A new instance of the record.
        """
        return await cls.executor(cls, *args, **kwargs)

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
        """Create a new record with some values replaced.

        Parameters
        ----------
        **to_replace : `T`
            The values to replace in the new record.

        Returns
        -------
        `Self`
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

    def __init__(self) -> None:
        pass

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
    
    See Also
    --------
    :class:`TableColumn`
    """
    _instances: ClassVar[dict[str, Self]] = {}
    """Container for all of the generators that were created."""
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
    def get_by_name(cls, name: str) -> 'TableColumnValueGenerator | None':
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
        `Self`
            A new instance of the generator with the same attributes.
        """
        return type(self)(name=self.name, generator=self.generator)
    
    def register(self) -> Self:
        """Register the copy of generator for global use.

        Returns
        -------
        `Self`
            Self for chaining.
        """
        self._instances[self.name] = self.copy()
        return self
    
    def unregister(self) -> Self:
        """Unregister the generator.

        Returns
        -------
        `Self`
            Self for chaining.
        """
        self._instances.pop(self.name, None)
        return self
    
    def __next__(self) -> T:
        """Retrieve a next value for the column.
        
        Returns
        -------
        `T`
            Generated value.
        """
        if isawaitable(result := self.generator()):
            return get_event_loop().run_until_complete(result)
        return result


@dataclass(slots=True, frozen=True)
class TableColumn[T]:
    """Represents a column in a table.

    This class defines the properties of a table column, including its name,
    data type, and constraints.

    .. Note::
        If both the ``generator`` and the ``default`` parameters are specified,
        preference is given to ``generator``.

    See Also
    --------
    :class:`TableColumnValueGenerator`
    :class:`Table`
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
    def from_sql[T](cls, sql: str, default: T | TableColumnValueGenerator[T] | None = None) -> 'TableColumn[T]':
        """Create a TableColumn instance from an SQL column definition.

        .. Note::
            Columns, that generated by sqlite using ``GENERATED`` keyword, won't be
            interpreted correctly. To make column generated, you need to create generator
            for it using the :class:`TableColumnValueGenerator` and then set column ``DEFAULT``
            value to ``"$Generated:generator_name"``

        Parameters
        ----------
        sql : `str`
            The SQL definition of the column.
        default : `T` | `None`, optional
            The default value or default value generator for the column.

        Returns
        -------
        :class:`TableColumn` [`T`]
            A new TableColumn instance.
        """
        name, typename = sql.split(' ', 2)[:2]
        default, generator = (None, default) if isinstance(default, TableColumnValueGenerator) else (default, None)
        return cls(name, typename, default, generator, 'PRIMARY KEY' in sql, 'UNIQUE' in sql, 'NOT NULL' not in sql)

    def to_sql(self) -> str:
        """Generate the SQL representation of the column.
        
        .. Note::
            - If column's typename is not present in :py:class:`.Parser` registry,
              it would be represented as ``TEXT``.
            - Columns with specified generator would have their ``DEFAULT`` value setted
              to ``"$Generated:generator_name"``.

        Returns
        -------
        `str`
            The SQL definition of the column.
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

    See Also
    --------
    :class:`TableColumn`
    :class:`TableRecord`
    :class:`MemoryTable`
    :class:`SolidTable`
    """
    name: str
    """The name of the table."""
    _columns: tuple[TableColumn[T], ...]
    """The columns of the table. This field is protected, use :attr:`Table.columns` instead."""
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
        """Create a new record for this table.
        
        For more details, see :meth:`TableRecord.make`.

        Parameters
        ----------
        *args : `T`
            Positional arguments for the record values.
        **kwargs : `T`
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
        """Insert a new record into the table.

        Parameters
        ----------
        ignore_existing : `bool`, optional
            Whether to ignore existing records. Defaults to False.
        **values : `T`
            The values for the new record.

        Returns
        -------
        :class:`TableRecord` [`T`]
            The inserted record.

        See Also
        --------
        :meth:`Table.insert_many`
        """

    async def insert_many(self, ignore_existing: bool = False, *values: dict[str, T]) -> AsyncIterator[TableRecord[T]]:
        """Insert multiple records into the table.

        .. Important::
            You must iterate over the result for operation to be performed.

        Parameters
        ----------
        ignore_existing : `bool`, optional
            Whether to ignore existing records. Defaults to False.
        *values : `dict` [`str`, `T`]
            Dictionaries containing the values for each record to insert.

        Yields
        ------
        :class:`TableRecord` [`T`]
            The inserted records.

        See Also
        --------
        :meth:`Table.insert`
        """
        for record_values in values:
            yield await self.insert(ignore_existing, **record_values)

    @abstractmethod
    def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Select records from the table.
        
        .. Note::
            If predicate isn't specified, yields the whole table.

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
        :meth:`Table.select_one`
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
        :meth:`Table.select`
        """
        return await anext(self.select(predicate), None)

    async def count(self, predicate: TableSelectionPredicate[T] | None = None) -> int:
        """Count the number of records in the table that match the predicate.

        .. Note::
            If no predicate is specified, the result will be the same as for the :meth:`Table.length` method.
        
        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records. Defaults to None.

        Returns
        -------
        `int`
            Number of records that match the predicate.
        """
        if predicate is not None:
            count = 0
            async for _ in self.select(predicate):
                count += 1
            return count
        return await self.length()
    
    @abstractmethod
    def deleted(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Remove and yield deleted records from the table.

        .. Note::
            If predicate isn't specified, yields the whole table and then clears it.

        .. Important::
            You must iterate over the result for operation to be performed.
        
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
        :meth:`Table.delete`
        :meth:`Table.delete_one`
        """

    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        """Delete records from the table.
        
        .. Note::
            If predicate isn't specified, clears the table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records to delete. Defaults to None.

        See Also
        --------
        :meth:`Table.deleted`
        :meth:`Table.delete_one`
        """
        async for _ in self.deleted(predicate):
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
        :meth:`Table.delete`
        :meth:`Table.delete_one`
        """
        return await anext(self.deleted(predicate), None)
    
    @abstractmethod
    def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        """Update records in the table and yield the updated records.
        
        .. Note::
            If predicate isn't specified, updates and yields every record.

        .. Important::
            You must iterate over the result for operation to be performed.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records to update. Defaults to None.
        **to_update : `T`
            The values to update in the matching records.

        Yields
        ------
        :class:`TableRecord` [`T`]
            The updated records.

        See Also
        --------
        :meth:`Table.update`
        :meth:`Table.update_one`
        """

    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        """Update records in the table without yielding the updated records.

        .. Note::
            If predicate isn't specified, upadates every record.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the records to update. Defaults to None.
        **to_update : `T`
            The values to update in the matching records.
        
        See Also
        --------
        :meth:`Table.updated`
        :meth:`Table.update_one`
        """
        async for _ in self.updated(predicate, **to_update):
            pass

    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> TableRecord[T] | None:
        """Update a single record in the table.

        Parameters
        ----------
        predicate : :class:`TableSelectionPredicate` [`T`] | `None`, optional
            A predicate to filter the record to update. Defaults to None.
        **to_update : `T`
            The values to update in the matching record.

        Returns
        -------
        :class:`TableRecord` [`T`] | `None`
            The updated record, or None if no record matches.

        See Also
        --------
        :meth:`Table.updated`
        :meth:`Table.update`
        """
        return await anext(self.updated(predicate, **to_update), None)
    
    async def __aenter__(self) -> Self:
        """Asynchronous context manager entry point.

        Entries the executor's context manager, allowing to interact with the table.

        Returns
        -------
        `Self`
            The Table instance.

        See Also
        --------
        :py:meth:`.ConsistentExecutor.__aenter__`
        """
        await self._executor.__aenter__()
        return self
    
    async def __aexit__(self, *args) -> None:
        """Asynchronous context manager exit point.

        Exits the executor's context manager and disables the table.

        Parameters
        ----------
        *args
            Arguments passed to the exit method.

        See Also
        --------
        :py:meth:`.ConsistentExecutor.__aexit__`
        """
        await self._executor.__aexit__(*args)
    

@dataclass(slots=True)
class MemoryTable[T](Table[T]):
    """A concrete implementation of Table for interacting with in-memory databases.

    This class provides methods for performing CRUD (Create, Read, Update, Delete) operations
    on 'memory tables' (actually, just a python sets). It implements the abstract methods
    defined in Table class.

    .. Warning::
        You must call :meth:`MemoryTable.start_executor` or enter table's async context using
        ``async with table`` construction before acessing the table, otherwise the  program will await
        for the request to complete forever.

    See Also
    --------
    :class:`TableColumn`
    :class:`TableRecord`
    :class:`Table`
    """
    _records: set[TableRecord[T]] = field(default_factory=set)
    """List of the table records."""

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
    
    async def deleted(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
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
    """Abstract base class for SQL-based tables.

    This class extends the functionality of the Table class to work with SQL databases.
    It provides methods for interacting with SQL tables and manages the connection
    to the database.

    See Also
    --------
    :class:`SolidTable`
    :class:`TableColumn`
    :class:`TableRecord`
    :class:`Table`
    """
    _executor: ConnectionManager

    @property
    def database(self) -> str:
        """Get the name of the database this table belongs to."""
        return self._executor.database

    @classmethod
    async def from_database[T](cls, name: str, executor: ConnectionManager) -> 'SqlTable[T]':        
        """Create a SqlTable instance from an existing database table.

        .. Warning::
            Before awaiting this function, make sure that the executor is running, otherwise the program will
            freeze.

        Parameters
        ----------
        name : `str`
            The name of the existing table in the database.
        executor : :py:class:`.ConnectionManager`
            The connection manager for the database.

        Returns
        -------
        :class:`SqlTable` [`T`]
            A new SqlTable instance representing the existing database table.

        Raises
        ------
        `AssertionError`
            If the table does not exist in the specified database.
        """
        table_sql = await (await executor.execute(f'SELECT sql FROM sqlite_master WHERE type="table" AND name="{name}"')).fetchone()
        assert table_sql is not None, f'Table "{name}" is not present in the "{executor.database}" database.'
        columns: list[TableColumn] = []
        table_sql: str = table_sql[0]
        columns_data = await executor.execute(f'SELECT type, dflt_value FROM pragma_table_info("{name}")')
        for column_sql in re_match(_ColumnsSqlFromTable, table_sql).group(1).split(', '):
            typename, default = await anext(columns_data)
            if isinstance(default, str) and (match_ := re_match(_RemoveQuotation, default)):
                default = match_.group(1)
            if isinstance(default, str) and default.startswith('$Generated:'):
                default = TableColumnValueGenerator.get_by_name(default[11:])
            else:
                try:
                    default = Parser.get_by_typename(typename).loads(default)
                except (TypeError, ValueError, AttributeError):
                    pass
            columns.append(TableColumn.from_sql(column_sql, default))
        return cls(name, tuple(columns), executor)

    @abstractmethod
    async def exists(self) -> bool:
        """Check if the table exists in the database.

        Returns
        -------
        `bool`
            `True` if the table is present in the database, otherwise `False`.
        """

    @abstractmethod
    async def create(self, if_not_exists: bool = True) -> None:
        """Create the SQL table in the database.

        Parameters
        ----------
        if_not_exists : `bool`, optional
            If `True`, the table will be created if it does not already exist without raising an
            exception. Defaults to `True`.
        """

    @abstractmethod
    async def drop(self, if_exists: bool = True) -> None:
        """Drop the SQL table from the database.

        Parameters
        ----------
        if_exists : `bool`, optional
            If `True`, the table will be dropped only if it already exists without raising an
            exception. Defaults to `True`.
        """


@dataclass(slots=True)
class SolidTable[T](SqlTable[T]):
    """A concrete implementation of SqlTable for interacting with SQLite databases.

    This class provides methods for performing CRUD (Create, Read, Update, Delete) operations
    on SQLite tables. It implements the abstract methods defined in SqlTable and Table classes.

    .. Warning::
        You must call :meth:`SolidTable.start_executor` or enter table's async context using
        ``async with table`` construction before acessing the table, otherwise the  program will await
        for the request to complete forever.

    See Also
    --------
    :class:`TableColumn`
    :class:`TableRecord`
    :class:`SqlTable`
    :class:`Table`
    """
    _default_selector: str = field(init=False)

    def __post_init__(self) -> None:
        super(SolidTable, self).__post_init__()
        self._default_selector = 'WHERE ' + ' AND '.join(f'{k} = ?' for k in self._record_type.fields)

    async def _execute_where(self, query: str, record: TableRecord[T], parameters: Parameters = ()) -> CursorManager:
        """Execute a SQL query with a WHERE clause based on the given record.

        Parameters
        ----------
        query : `str`
            The SQL query to execute.
        record : :class:`TableRecord` [`T`]
            The record to use for the WHERE clause.
        parameters : :py:data:`.Parameters`, optional
            Additional parameters for the query.

        Returns
        -------
        :class:`CursorManager`
            A cursor manager for the executed query.
        """
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
    
    async def deleted(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
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

    async def exists(self) -> bool:
        return await bool((await self._executor.execute(
            f'SELECT COUNT(name) FROM sqlite_master WHERE type="table" AND name="{self.name}"'
        )).fetchone()[0])

    async def create(self, if_not_exists: bool = True) -> None:
        await self._executor.execute(
            'CREATE TABLE ' +
            ('IF NOT EXISTS ' if if_not_exists else '') +
            f'"{self.name}" ({", ".join(column.to_sql() for column in self.columns)})'
        )
    
    async def drop(self, if_exists: bool = True) -> None:
        await self._executor.execute(
            'DROP TABLE ' +
            ('IF EXISTS ' if if_exists else '') +
            f'"{self.name}"'
        )
