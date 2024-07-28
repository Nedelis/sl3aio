from typing import AsyncIterable, AsyncGenerator, List, Any
from asyncio import gather
from re import search, IGNORECASE
from .executor import _ExecutorFactory, single_executor
from .dataparser import Parser


async def azip[T](*iterables: AsyncIterable[T], strict: bool = False) -> AsyncGenerator[List[T], Any]:
    while True:
        try:
            yield await gather(*(anext(it) for it in iterables))
        except StopAsyncIteration:
            if strict:
                for it in iterables:
                    if next(it, None) is not None:
                        raise ValueError(f'azip() arguments have different lengths!')
            break


async def columns_sql(database: str, table: str, executor_factory: _ExecutorFactory = single_executor) -> AsyncGenerator[str, Any]:
    sql = (await executor_factory(database)(f'SELECT sql FROM sqlite_master WHERE type = "table" AND name = "{table}"')).fetchone()
    if sql is None:
        raise ValueError(f'There is no table "{table}" in database "{database}"!')
    if match_ := search(r'CREATE TABLE\s+\w+\s*\((.*)\)', sql[0], IGNORECASE):
        for column_sql in match_.group(1).split(', '):
            yield column_sql


async def columns_defaults(database: str, table: str, executor_factory: _ExecutorFactory = single_executor) -> AsyncGenerator[str, Any]:
    for alias, default in await executor_factory(database)(f'SELECT type, dflt_value FROM pragma_table_info("{table}")'):
        default = default.strip('\'"`') if isinstance(default, str) else default
        try:
            yield Parser.get_by_typename(alias).loads(default)
            continue
        except (TypeError, ValueError, AttributeError):
            yield default
