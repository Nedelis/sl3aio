import operator
from math import trunc, floor, ceil
from functools import partial
from collections.abc import AsyncIterator, Callable, Container
from dataclasses import dataclass, replace
from typing import Any, Self, Concatenate
from .dataparser import Parser
from .table import Table, TableColumn, TableRecord, TableSelectionPredicate, TableColumnValueGenerator

__all__ = ['EasySelector', 'EasyColumn', 'EasyTable']


def _default_selector(previous, _) -> tuple[bool, Any]:
    return True, previous


@dataclass(slots=True, frozen=True)
class EasySelector[T]:
    table: Table[T] | None = None
    _selector: Callable[[Any, TableRecord[T]], tuple[bool, Any]] = _default_selector

    @property
    def predicate(self) -> TableSelectionPredicate[T]:
        def __predicate(record: TableRecord[T]) -> bool:
            nonlocal self
            return self.apply(record)[0]
        return __predicate
    
    def pin_table(self, table: Table[T]) -> Self:
        return replace(self, table=table)

    def apply(self, record: TableRecord[T]) -> tuple[bool, Any]:
        return self._selector(record, record)

    async def select(self, table: Table[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        async with (table or self.table) as table:
            async for record in table.select(self.predicate):
                yield record
    
    async def select_one(self, table: Table[T] | None = None) -> TableRecord[T] | None:
        return await anext(self.select(table), None)
    
    async def pop(self, table: Table[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        async with (table or self.table) as table:    
            async for record in table.pop(self.predicate):
                yield record
    
    async def delete(self, table: Table[T] | None = None) -> None:
        async for _ in self.pop(table):
            pass

    async def delete_one(self, table: Table[T] | None = None) -> TableRecord[T] | None:
        return await anext(self.pop(table), None)
    
    async def updated(self, table: Table[T], **to_update: T) -> AsyncIterator[TableRecord[T]]:
        async with (table or self.table) as table:
            async for record in table.updated(self.predicate, **to_update):
                yield record

    async def update(self, table: Table[T], **to_update: T) -> None:
        async for _ in self.updated(table, **to_update):
            pass

    async def update_one(self, table: Table[T], **to_update: T) -> TableRecord[T] | None:
        return await anext(self.updated(table, **to_update), None)

    def append_selector(self, selector: Callable[[bool, Any, TableRecord[T]], tuple[bool, Any]]) -> Self:
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, selector
            ok, obj = self._selector(previous, record)
            return selector(ok, obj, record)
        return replace(self, _selector=__selector)

    def pass_into[**P](self, func: Callable[Concatenate[Any, P], Any], *args: P.args, **kwargs: P.kwargs) -> Self:
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, func, args, kwargs
            ok, obj = self._selector(previous, record)
            return (True, func(obj, *args, **kwargs)) if ok else (False, obj)
        return replace(self, _selector=__selector)
    
    def set_ok(self, value: bool = True) -> Self:
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, value
            return value, self._selector(previous, record)[1]
        return replace(self, _selector=__selector)
    
    def in_(self, container: Container | Self) -> Self:
        if isinstance(container, self.__class__):
            return self in container
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, container
            ok, obj = self._selector(previous, record)
            return (True, True) if ok and obj in container else (False, False)
        return replace(self, _selector=__selector)
    
    def and_(self, other: Self) -> Self:
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
    
    def __contains__(self, item: Any | Self) -> bool:
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
    
    def _binary_operator(self, other: Any | Self, operator: Callable[[Any, Any], Any]) -> Self:
        if isinstance(other, self.__class__):
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, operator
                ok, obj = self._selector(previous, record)
                if ok and (_other := other.apply(record))[0]:
                    return True, operator(obj, _other[1])
                return False, obj
        else:
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, operator
                ok, obj = self._selector(previous, record)
                return (True, operator(obj, other)) if ok else (False, obj)
        return replace(self, _selector=__selector)
    
    def _rbinary_operator(self, other: Any | Self, operator: Callable[[Any, Any], Any]) -> Self:
        def _operator(a, b):
            nonlocal operator
            return operator(b, a)
        return self._binary_operator(other, _operator)

    def _unary_operator(self, operator: Callable[[Any], Any]) -> Self:
        def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
            nonlocal self, operator
            ok, obj = self._selector(previous, record)
            return (True, operator(obj)) if ok else (False, obj)
        return replace(self, _selector=__selector)
    
    def _comparaison_operator(self, other: Any | Self, comparator: Callable[[Any, Any], bool]) -> Self:
        if isinstance(other, self.__class__):
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, comparator
                ok, obj = self._selector(previous, record)
                if ok and (_other := other.apply(record))[0] and comparator(obj, _other[1]):
                    return True, True
                return False, False
        else:
            def __selector(previous, record: TableRecord[T]) -> tuple[bool, Any]:
                nonlocal self, other, comparator
                ok, obj = self._selector(previous, record)
                return (True, True) if ok and comparator(obj, other) else (False, False)
        return replace(self, _selector=__selector)
    

@dataclass(slots=True, frozen=True)
class EasyColumn[T]:
    default: T | TableColumnValueGenerator[T] | None = None
    primary: bool = False
    unique: bool = False
    nullable: bool = True

    def to_column(self, name: str, __type: type[T]) -> TableColumn[T]:
        assert (parser := Parser.get_by_type(__type)), f'Invalid type {__type}!'
        return TableColumn(
            name,
            parser.typenames[0],
            *((None, self.default) if isinstance(self.default, TableColumnValueGenerator) else (self.default, None)),
            self.primary,
            self.unique,
            self.nullable
        )


class EasyTable[T]:
    """Case of use:
    >>> from sl3aio import EasyTable, EasyColumn, TableColumnValueGenerator
    >>> from operator import call
    >>> @call
    >>> class Person(EasyTable):
    ...     id: int = EasyColumn(TableColumnValueGenerator.get('id_increment'), primary=True, nullable=False)
    ...     name: str = 'unknown_person'
    ...     age: int = EasyColumn(nullable=False)
    ...     email: str
    >>> columns = Person.columns
    >>> person_age_selector = Person.age
    """
    __slots__ = 'table', '_columns'
    _columns: tuple[TableColumn[T], ...]
    table: Table[T]

    def __init__(self, table: Table[T] | None = None) -> None:
        columns = []
        for column_name, column_type in self.__annotations__.items():
            if column_name in self.__slots__:
                continue
            if not isinstance(value := getattr(self.__class__, column_name, None), EasyColumn):
                value = EasyColumn(value)
            try:
                delattr(self.__class__, column_name)
            except AttributeError:
                pass
            columns.append(value.to_column(column_name, column_type))
        self._columns = tuple(columns)
        self.table = table

    @property
    def columns(self) -> tuple[TableColumn[T], ...]:
        return self._columns
    
    async def contains(self, record: TableRecord[T]) -> bool:
        async with self.table:
            return self.table.contains(record)
        
    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        async with self.table:
            return await self.table.insert(ignore_existing, **values)
        
    async def insert_many(self, ignore_existing: bool = False, *values: dict[str, T]) -> AsyncIterator[TableRecord[T]]:
        async with self.table:
            async for record in self.table.insert_many(ignore_existing, *values):
                yield record
        
    async def select(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        async with self.table:
            async for record in self.table.select(predicate.predicate if isinstance(predicate, EasySelector) else predicate):
                yield record

    async def select_one(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None) -> TableRecord[T] | None:
        return await anext(self.select(predicate), None)
    
    async def pop(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        async with self.table:
            async for record in self.table.pop(predicate.predicate if isinstance(predicate, EasySelector) else predicate):
                yield record
    
    async def delete(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None) -> None:
        async for _ in self.pop(predicate):
            pass

    async def delete_one(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None) -> TableRecord[T] | None:
        return await anext(self.pop(predicate), None)
    
    async def updated(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        async with self.table:
            async for record in self.table.updated(predicate.predicate if isinstance(predicate, EasySelector) else predicate, **to_update):
                yield record

    async def update(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None, **to_update: T) -> None:
        async for _ in self.updated(predicate, **to_update):
            pass

    async def update_one(self, predicate: TableSelectionPredicate[T] | EasySelector[T] | None = None, **to_update: T) -> TableRecord[T] | None:
        return await anext(self.updated(predicate, **to_update), None)

    def __getattr__(self, name: str) -> EasySelector[T]:
        return getattr(EasySelector(self.table), name)
