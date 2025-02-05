"""
sl3aio.easytable
================

This module provides a high-level, user-friendly interface for working with database tables in the sl3aio library. 
It offers simplified abstractions for common database operations, making it easier to define, query, and manipulate 
database tables.

Key Components:
---------------
1. EasySelector: A powerful class for building complex database queries and selections.
2. EasyColumn: A simplified way to define table columns with various attributes.
3. EasyTable: A high-level representation of database tables with easy-to-use methods for common operations.

The module aims to provide a more intuitive and Pythonic way of interacting with database tables, reducing 
the complexity often associated with SQL operations.

Features:
---------
- Simplified table and column definitions
- Fluent interface for building complex queries
- Easy-to-use methods for common database operations (insert, select, update, delete)

Usage:
------
This module is designed to be used in conjunction with other components of the sl3aio library. It's particularly 
useful for developers who want a more abstract and Pythonic way of working with database tables, without dealing 
directly with SQL or low-level database operations.

Examples:
---------
>>> class User(EasyTable[int | str]):
...     id: int = EasyColumn(TableColumnValueGenerator('randomID'), primary=True, nullable=False)
...     name: str = 'John Doe'
...     age: int = EasyColumn(nullable=False)
...     email: str

>>> table = MemoryTable('user', User.columns())
>>> User = User(table=table)
>>> await (User.id == 18798561).select_one()

This example demonstrates how to define a table, create an instance, and perform a simple query.

Note:
-----
This module is part of the sl3aio library and is designed to work seamlessly with other components of the library. 
It's recommended to familiarize yourself with the basic concepts of sl3aio before using this module extensively.
"""
import operator
from math import trunc, floor, ceil
from functools import partial
from collections.abc import AsyncIterator, Callable, Container
from dataclasses import dataclass, replace
from typing import Any, Self, Concatenate, get_args
from .dataparser import Parser
from .table import Table, TableColumn, TableRecord, TableSelectionPredicate, TableColumnValueGenerator

__all__ = ['EasySelector', 'EasyColumn', 'EasyTable']


def _default_selector(previous, _) -> tuple[bool, Any]:
    return True, previous


@dataclass(slots=True, frozen=True)
class EasySelector[T]:
    """A class for creating and manipulating selectors for database operations.

    This class provides methods for building complex selection criteria and performing
    various database operations based on those criteria.

    Attributes
    ----------
    table : Table[T], None, optional
        The database table to operate on. Defaults to None.
    _selector : Callable[[Any, TableRecord[T]], tuple[bool, Any]], optional
        The selector function. Defaults to :func:`sl3aio.easytable._default_selector`
    """
    table: Table[T] | None = None
    _selector: Callable[[Any, TableRecord[T]], tuple[bool, Any]] = _default_selector

    @property
    def predicate(self) -> TableSelectionPredicate[T]:
        """Creates a predicate function based on the current selector.

        Returns
        -------
        TableSelectionPredicate[T]
            An async function that takes a TableRecord[T] and returns a boolean.
        """
        async def __predicate(record: TableRecord[T]) -> bool:
            nonlocal self
            return (await record.executor(self.apply, record))[0]
        return __predicate
    
    def pin_table(self, table: Table[T]) -> Self:
        """Creates a new EasySelector with the specified table.

        Parameters
        ----------
        table : Table[T]
            The table to pin to the selector.

        Returns
        -------
        Self
            A new EasySelector instance with the specified table.
        """
        return replace(self, table=table)

    def apply(self, record: TableRecord[T]) -> tuple[bool, Any]:
        """Applies the selector to a given record.

        Parameters
        ----------
        record : TableRecord[T]
            The record to apply the selector to.

        Returns
        -------
        tuple[bool, Any]
            A tuple containing a boolean indicating if the selector matched, and the result of the selector application.
        """

        return self._selector(record, record)

    async def select(self, table: Table[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Selects records from the table based on the current selector.

        Parameters
        ----------
        table : Table[T], None, optional
            The table to select from. If None, uses the pinned table. Defaults to None.

        Returns
        -------
        AsyncIterator[TableRecord[T]]
            An async iterator of selected records.
        """
        async with (table or self.table) as table:
            async for record in table.select(self.predicate):
                yield record
    
    async def select_one(self, table: Table[T] | None = None) -> TableRecord[T] | None:
        """Selects a single record from the table based on the current selector.

        Parameters
        ----------
        table : Table[T], None, optional
            The table to select from. If None, uses the pinned table. Defaults to None.

        Returns
        -------
        TableRecord[T], None
            The selected record, or None if no record matches.
        """
        return await anext(self.select(table), None)
    
    async def pop(self, table: Table[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Selects and removes records from the table based on the current selector.

        Parameters
        ----------
        table : Table[T], None, optional
            The table to pop from. If None, uses the pinned table. Defaults to None.

        Returns
        -------
        AsyncIterator[TableRecord[T]]
            An async iterator of popped records.
        """
        async with (table or self.table) as table:    
            async for record in table.pop(self.predicate):
                yield record
    
    async def delete(self, table: Table[T] | None = None) -> None:
        """Deletes all records from the table that match the current selector.

        Parameters
        ----------
        table : Table[T], None, optional
            The table to delete from. If None, uses the pinned table. Defaults to None.
        """
        async for _ in self.pop(table):
            pass

    async def delete_one(self, table: Table[T] | None = None) -> TableRecord[T] | None:
        """Deletes a single record from the table that matches the current selector.

        Parameters
        ----------
        table : Table[T], None, optional
            The table to delete from. If None, uses the pinned table. Defaults to None.

        Returns
        -------
        TableRecord[T], None
            The deleted record, or None if no record matches.
        """
        return await anext(self.pop(table), None)
    
    async def updated(self, table: Table[T], **to_update: T) -> AsyncIterator[TableRecord[T]]:
        """Updates records in the table that match the current selector.

        Parameters
        ----------
        table : Table[T]
            The table to update.
        to_update : T
            Keyword arguments specifying the fields to update and their new values.

        Returns
        -------
        AsyncIterator[TableRecord[T]]
            An async iterator of updated records.
        """
        async with (table or self.table) as table:
            async for record in table.updated(self.predicate, **to_update):
                yield record

    async def update(self, table: Table[T], **to_update: T) -> None:
        """Updates all records in the table that match the current selector.

        Parameters
        ----------
        table : Table[T]
            The table to update.
        to_update : T
            Keyword arguments specifying the fields to update and their new values.
        """
        async for _ in self.updated(table, **to_update):
            pass

    async def update_one(self, table: Table[T], **to_update: T) -> TableRecord[T] | None:
        """Updates a single record in the table that matches the current selector.

        Parameters
        ----------
        table : Table[T]
            The table to update.
        to_update : T
            Keyword arguments specifying the fields to update and their new values.

        Returns
        -------
        TableRecord[T], None
            The updated record, or None if no record matches.
        """
        return await anext(self.updated(table, **to_update), None)

    def append_selector(self, selector: Callable[[bool, Any, TableRecord[T]], tuple[bool, Any]]) -> Self:
        """Appends a new selector to the current selector chain.

        Parameters
        ----------
        selector : Callable[[bool, Any, TableRecord[T]], tuple[bool, Any]]
            The selector to append.

        Returns
        -------
        EasySelector
            A new EasySelector instance with the appended selector.
        """
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, selector
            ok, obj = self._selector(previous, record)
            return selector(ok, obj, record)
        return replace(self, _selector=__selector)

    def pass_into[**P](self, func: Callable[Concatenate[Any, P], Any], *args: P.args, key_or_pos: str | int = 0, **kwargs: P.kwargs) -> Self:
        """Passes the result of the current selector into a function.

        Parameters
        ----------
        func : Callable[Concatenate[Any, P], Any]
            The function to pass the result into.
        key_or_pos : int, str, optional
            Argument name or position in the function's signature.  Defaults to 0.
        args : P.args
            Positional arguments to pass to the function.
        kwargs : P.kwargs
            Keyword arguments to pass to the function.

        Returns
        -------
        EasySelector
            A new EasySelector instance with the modified selector.
        """
        if isinstance(key_or_pos, str):
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, func, key_or_pos, args, kwargs
                ok, obj = self._selector(previous, record)
                return (True, func(*args, **(kwargs | {key_or_pos: obj}))) if ok else (False, obj)
        else:
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, func, key_or_pos, args, kwargs
                ok, obj = self._selector(previous, record)
                return (True, func(*args[:key_or_pos], obj, *args[key_or_pos + 1:], **kwargs)) if ok else (False, obj)
        return replace(self, _selector=__selector)
    
    def set_ok(self, value: bool = True) -> Self:
        """Sets the 'ok' status of the selector to a fixed value.

        Parameters
        ----------
        value : bool, optional
            The value to set for the 'ok' status. Defaults to True.

        Returns
        -------
        EasySelector
            A new EasySelector instance with the modified selector.
        """
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, value
            return value, self._selector(previous, record)[1]
        return replace(self, _selector=__selector)
    
    def in_(self, container: Container | Self) -> Self:
        """Checks if the result of the current selector is in a container.

        Parameters
        ----------
        container : Container, Self
            The container to check against.

        Returns
        -------
        EasySelector
            A new EasySelector instance with the modified selector.
        """
        if isinstance(container, self.__class__):
            return self in container
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, container
            ok, obj = self._selector(previous, record)
            return (True, True) if ok and obj in container else (False, False)
        return replace(self, _selector=__selector)
    
    def and_(self, other: Self) -> Self:
        """Combines the current selector with another using logical AND.

        Parameters
        ----------
        other : Self
            The other selector to combine with.

        Returns
        -------
        EasySelector
            A new EasySelector instance representing the combined selector.
        """
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, other
            ok, obj = self._selector(previous, record)
            if not ok:
                return False, obj
            elif not (_other := other.apply(record))[0]:
                return False, _other[1]
            return True, _other[1]
        return replace(self, _selector=__selector)
    
    def or_(self, other: Self) -> Self:
        """Combines the current selector with another using logical OR.

        Parameters
        ----------
        other : Self
            The other selector to combine with.

        Returns
        -------
        EasySelector
            A new EasySelector instance representing the combined selector.
        """
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, other
            ok, obj = self._selector(previous, record)
            if ok:
                return True, obj
            elif (_other := other.apply(record))[0]:
                return True, _other[1]
            return False, _other[1]
        return replace(self, _selector=__selector)
    
    def not_(self) -> Self:
        """Negates the current selector.

        Returns
        -------
        EasySelector
            A new EasySelector instance representing the negated selector.
        """
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self
            return (True, previous) if not self._selector(previous, record)[0] else (False, previous)
        return replace(self, _selector=__selector)
    
    def __getattr__(self, name: str) -> Self:
        return self._binary_operator(name, getattr)
    
    def __getitem__(self, item: Any | Self) -> Self:
        return self._binary_operator(item, operator.getitem)
    
    def __reversed__(self) -> Self:
        return self._unary_operator(reversed)
    
    def __call__(self, *args, **kwargs) -> Self:
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, args, kwargs
            ok, obj = self._selector(previous, record)
            if not ok or (isinstance(result := obj(*args, **kwargs), bool) and not result):
                return False, previous
            return True, result
        return replace(self, _selector=__selector)

    def __eq__(self, other: Any | Self) -> Self:
        return self._comparaison_operator(other, operator.eq)
    
    def __ne__(self, other: Any | Self) -> Self:
        return self._comparaison_operator(other, operator.ne)
    
    def __lt__(self, other: Any | Self) -> Self:
        return self._comparaison_operator(other, operator.lt)
    
    def __le__(self, other: Any | Self) -> Self:
        return self._comparaison_operator(other, operator.le)
    
    def __gt__(self, other: Any | Self) -> Self:
        return self._comparaison_operator(other, operator.gt)
    
    def __ge__(self, other: Any | Self) -> Self:
        return self._comparaison_operator(other, operator.ge)
    
    def __contains__(self, item: Any | Self) -> Self:
        return self._comparaison_operator(item, operator.contains)
    
    def __add__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.add)
    
    def __radd__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.add)
    
    def __sub__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.sub)
    
    def __rsub__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.sub)
    
    def __mul__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.mul)
    
    def __rmul__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.mul)
    
    def __matmul__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.matmul)
    
    def __rmatmul__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.matmul)
    
    def __truediv__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.truediv)
    
    def __rtruediv__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.truediv)
    
    def __floordiv__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.floordiv)
    
    def __rfloordiv__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.floordiv)
    
    def __mod__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.mod)
    
    def __rmod__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.mod)
    
    def __pow__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.pow)
    
    def __rpow__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.pow)
    
    def __divmod__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, divmod)
    
    def __neg__(self) -> Self:
        return self._unary_operator(operator.neg)
    
    def __pos__(self) -> Self:
        return self._unary_operator(operator.pos)
    
    def __abs__(self) -> Self:
        return self._unary_operator(operator.abs)
    
    def __complex__(self) -> Self:
        return self._unary_operator(complex)
    
    def __int__(self) -> Self:
        return self._unary_operator(int)
    
    def __float__(self) -> Self:
        return self._unary_operator(float)
    
    def __round__(self, n: int = 0) -> Self:
        return self._unary_operator(partial(round, ndigits=n))
    
    def __trunc__(self) -> Self:
        return self._unary_operator(trunc)
    
    def __floor__(self) -> Self:
        return self._unary_operator(floor)
    
    def __ceil__(self) -> Self:
        return self._unary_operator(ceil)
    
    def __invert__(self) -> Self:
        return self._unary_operator(operator.invert)
    
    def __lshift__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.lshift)
    
    def __rlshift__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.lshift)
        
    def __rshift__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.rshift)
    
    def __rrshift__(self, other: Any | Self) -> Self:
        return self._rbinary_operator(other, operator.rshift)
    
    def __and__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.and_)
    
    def __rand__(self, other) -> Self:
        return self._rbinary_operator(other, operator.and_)

    def __or__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.or_)
    
    def __ror__(self, other) -> Self:
        return self._rbinary_operator(other, operator.or_)
    
    def __xor__(self, other: Any | Self) -> Self:
        return self._binary_operator(other, operator.xor)
    
    def __rxor__(self, other) -> Self:
        return self._rbinary_operator(other, operator.xor)
    
    def _binary_operator(self, other: Any | Self, __operator: Callable[[Any, Any], Any]) -> Self:
        if isinstance(other, self.__class__):
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, __operator
                ok, obj = self._selector(previous, record)
                if ok and (_other := other.apply(record))[0]:
                    return True, __operator(obj, _other[1])
                return False, obj
        else:
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, __operator
                ok, obj = self._selector(previous, record)
                return (True, __operator(obj, other)) if ok else (False, obj)
        return replace(self, _selector=__selector)
    
    def _rbinary_operator(self, other: Any | Self, __operator: Callable[[Any, Any], Any]) -> Self:
        def __roperator(a, b):
            nonlocal __operator
            return __operator(b, a)
        return self._binary_operator(other, __roperator)

    def _unary_operator(self, __operator: Callable[[Any], Any]) -> Self:
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, __operator
            ok, obj = self._selector(previous, record)
            return (True, __operator(obj)) if ok else (False, obj)
        return replace(self, _selector=__selector)
    
    def _comparaison_operator(self, other: Any | Self, comparator: Callable[[Any, Any], bool]) -> Self:
        if isinstance(other, self.__class__):
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, comparator
                ok, obj = self._selector(previous, record)
                _other = other.apply(record)
                return (True, True) if ok and _other[0] and comparator(obj, _other[1]) else (False, False)
        else:
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, comparator
                ok, obj = self._selector(previous, record)
                return (True, True) if ok and comparator(obj, other) else (False, False)
        return replace(self, _selector=__selector)
    

@dataclass(slots=True, frozen=True)
class EasyColumn[T]:
    """Represents an easy-to-use column definition for database tables.

    This class provides a simplified way to define columns for database tables,
    including options for default values, primary key, uniqueness, and nullability.

    Attributes
    ----------
    default : T, TableColumnValueGenerator[T], None, optional
        The default value for the column. Can be a static value, a TableColumnValueGenerator, or None.
        Defaults to None.
    primary : bool, optional
        Indicates if this column is a primary key. Defaults to False.
    unique : bool, optional
        Indicates if this column should have unique values. Defaults to False.
    nullable : bool, optional
        Indicates if this column can contain NULL values. Defaults to True.
    
    Methods
    -------
    to_column
        Converts the EasyColumn instance to a TableColumn instance.
    """
    default: T | TableColumnValueGenerator[T] | None = None
    primary: bool = False
    unique: bool = False
    nullable: bool = True

    def to_column(self, name: str, __type: type[T]) -> TableColumn[T]:
        """Converts the EasyColumn instance to a TableColumn instance.

        This method creates a TableColumn object based on the EasyColumn's attributes
        and the provided name and type.

        Parameters
        ----------
        name : str
            The name of the column.
        __type : type[T]
            The Python type of the column's values.

        Returns
        -------
        TableColumn[T]
            A TableColumn instance representing the column in the database.

        Note
        ----
        If a Parser is not found for the given type, 'TEXT' is used as the default type.
        """
        return TableColumn(
            name,
            next(iter(parser.typenames)) if (parser := Parser.get_by_type(__type)) else 'TEXT',
            *((None, self.default) if isinstance(self.default, TableColumnValueGenerator) else (self.default, None)),
            self.primary,
            self.unique,
            self.nullable
        )


@dataclass(slots=True)
class EasyTable[T]:
    """A class representing an easy-to-use selection interface for database tables.

    This class provides methods for common database operations such as inserting,
    selecting, updating, and deleting records.

    Attributes
    ----------
    table : Table[T], None, optional
        The underlying database table. Defaults to None.

    Examples
    --------
    In the next example you can see the way to create a markup using EasyTable, convert it into a
    working table and then access it using EasyTable functions.

    .. code-block:: python

        from sl3aio import EasyColumn, EasyTable, MemoryTable, TableColumnValueGenerator, EasySelector
        from random import randint
        from asyncio import run


        # Custom value generator for user IDs. See TableColumnValueGenerator.
        @TableColumnValueGenerator.from_function('userID')
        def __generate_user_id() -> int:
            return randint(1000000, 9999999)


        # Define the structure of the Users table using EasyTable
        # EasyTable[int | str] indicates that column values can be either int or str
        class UsersTableMarkup(EasyTable[int | str]):
            # Define columns with their types, properties, and constraints
            # EasySelector[int] specifies that this column will contain integer values
            id: EasySelector[int] = EasyColumn(TableColumnValueGenerator('userID'), primary=True, nullable=False)
            # 'John Doe' is set as the default value for the name column
            name: EasySelector[str] = 'John Doe'
            # age column is marked as non-nullable, requiring a value for every record
            age: EasySelector[int] = EasyColumn(nullable=False)
            # email column is defined without additional constraints
            email: EasySelector[str]


        async def main():
            # Create a MemoryTable instance based on the UsersTableMarkup
            # This step converts the markup into actual table columns
            columns = UsersTableMarkup.columns()
            # Initialize an in-memory table named 'users' with the defined columns
            table = MemoryTable('users', columns)
            # Create a high-level interface for interacting with the users table
            users_table = UsersTableMarkup(table)

            # Insert a new user record into the table
            # The 'id' field is automatically generated using the custom generator
            await users_table.insert(name='Foo Bar', age=23, email='foobar@gmail.com')

            # Demonstrate querying capabilities
            # This line performs these operations:
            # 1. Create a query condition: users_table.name == 'Foo Bar'
            # 2. Select one record matching this condition: .select_one()
            # 3. Access the 'email' field of the selected record
            # 4. Print the result
            print((await (users_table.name == 'Foo Bar').select_one()).email)


        run(main())  # >>> foobar@gmail.com

    """
    table: Table[T]

    @classmethod
    def columns(cls) -> tuple[TableColumn[T], ...]:
        """Get the columns of the table from the fields of the subclass.

        Returns
        -------
        tuple[TableColumn[T], ...]
            A tuple of TableColumn objects representing the table's columns.
        """
        columns = []
        for column_name, column_type in cls.__annotations__.items():
            if column_name in cls.__slots__:
                continue
            if not isinstance(value := getattr(cls, column_name, None), EasyColumn):
                value = EasyColumn(value)
            try:
                delattr(cls, column_name)
            except AttributeError:
                pass
            columns.append(value.to_column(column_name, get_args(column_type)[0]))
        return tuple(columns)
    
    async def contains(self, record: TableRecord[T]) -> bool:
        """Check if the table contains a specific record.

        Parameters
        ----------
        record : TableRecord[T]
            The record to check for.

        Returns
        -------
        bool
            True if the record is in the table, False otherwise.
        """
        async with self.table:
            return await self.table.contains(record)
        
    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        """Insert a new record into the table.

        Parameters
        ----------
        ignore_existing : bool, optional
            If True, ignore if the record already exists. Defaults to False.
        values : T
            The values to insert, specified as keyword arguments.

        Returns
        -------
        TableRecord[T]
            The inserted record.
        """
        async with self.table:
            return await self.table.insert(ignore_existing, **values)
        
    async def insert_many(self, ignore_existing: bool = False, *values: dict[str, T]) -> AsyncIterator[TableRecord[T]]:
        """Insert multiple records into the table.

        Parameters
        ----------
        ignore_existing : bool, optinal
            If True, ignore if the records already exist. Defaults to False.
        values : dict[str, T]
            The values to insert, specified as dictionaries.

        Returns
        -------
        AsyncIterator[TableRecord[T]]
            An async iterator of the inserted records.
        """
        async with self.table:
            async for record in self.table.insert_many(ignore_existing, *values):
                yield record
        
    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Select records from the table based on a predicate.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.

        Returns
        -------
        AsyncIterator[TableRecord[T]]
            An async iterator of the selected records.
        """
        async with self.table:
            async for record in self.table.select(predicate):
                yield record

    async def select_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        """Select a single record from the table based on a predicate.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.

        Returns
        -------
        TableRecord[T], None
            The selected record, or None if no record matches the predicate.
        """
        return await anext(self.select(predicate), None)
    
    async def pop(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        """Remove and return records from the table based on a predicate.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.

        Returns
        -------
        AsyncIterator[TableRecord[T]]
            An async iterator of the removed records.
        """
        async with self.table:
            async for record in self.table.pop(predicate):
                yield record
    
    async def delete(self, predicate: TableSelectionPredicate[T] | None = None) -> None:
        """Delete records from the table based on a predicate.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.
        """
        async for _ in self.pop(predicate):
            pass

    async def delete_one(self, predicate: TableSelectionPredicate[T] | None = None) -> TableRecord[T] | None:
        """Delete a single record from the table based on a predicate.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.

        Returns
        -------
        TableRecord[T], None
            The deleted record, or None if no record matches the predicate.
        """
        return await anext(self.pop(predicate), None)
    
    async def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        """Update records in the table based on a predicate and return the updated records.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.
        to_update : T
            The values to update, specified as keyword arguments.

        Returns
        -------
        AsyncIterator[TableRecord[T]]
            An async iterator of the updated records.
        """
        async with self.table:
            async for record in self.table.updated(predicate, **to_update):
                yield record

    async def update(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> None:
        """Update records in the table based on a predicate.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.
        to_update : T
            The values to update, specified as keyword arguments.
        """
        async for _ in self.updated(predicate, **to_update):
            pass

    async def update_one(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> TableRecord[T] | None:
        """Update a single record in the table based on a predicate.

        Parameters
        ----------
        predicate : TableSelectionPredicate[T], None, optional
            The selection predicate. Defaults to None.
        to_update : T
            The values to update, specified as keyword arguments.

        Returns
        -------
        TableRecord[T], None
            The updated record, or None if no record matches the predicate.
        """
        return await anext(self.updated(predicate, **to_update), None)

    def __getattr__(self, name: str) -> EasySelector[T]:
        """Get an EasySelector for a specific attribute of the table.

        Parameters
        ----------
        name : str
            The name of the attribute.

        Returns
        -------
        EasySelector[T]
            An EasySelector instance for the specified attribute.
        """
        return getattr(EasySelector(self.table), name)
