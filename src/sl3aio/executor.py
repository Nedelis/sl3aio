from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from asyncio import AbstractEventLoop, Future, Queue, Task, get_running_loop, create_task
from functools import partial
from pathlib import Path
from sqlite3 import Cursor, connect, Connection
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping
from typing import Any, ClassVar, Self
from .dataparser import DefaultDataType

__all__ = ['Parameters', 'Executor', 'ConsistentExecutor', 'ConnectionManager', 'CursorManager']

type Parameters = Iterable[DefaultDataType] | Mapping[str, DefaultDataType]


@dataclass(slots=True)
class Executor:
    _loop: AbstractEventLoop = field(init=False, default_factory=get_running_loop)
    _executor: ThreadPoolExecutor = field(init=False, default_factory=ThreadPoolExecutor)

    def __call__[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        return self._loop.run_in_executor(self._executor, partial(func, *args, **kwargs))


@dataclass(slots=True)
class ConsistentExecutor(Executor):
    _executor: ThreadPoolExecutor = field(init=False, default_factory=partial(ThreadPoolExecutor, max_workers=1))
    _queue: Queue[tuple[Callable[[], Any], Future] | None] = field(init=False, default_factory=Queue)
    _worker_task: Task | None = field(init=False, default=None)
    _refcount: int = field(init=False, default=0)

    @property
    def running(self) -> bool:
        return self._worker_task is not None

    async def _worker(self) -> None:
        while True:
            task = await self._queue.get()
            if task is None:
                self._queue.task_done()
                break
            elif task[1].done():
                self._queue.task_done()
                continue
            try:
                task[1].set_result(await self._loop.run_in_executor(self._executor, task[0]))
            except Exception as e:
                task[1].set_exception(e)
            finally:
                self._queue.task_done()
    
    async def start(self) -> None:
        self._refcount += 1
        if self._worker_task is None:
            self._worker_task = create_task(self._worker())

    async def stop(self) -> None:
        if self._refcount == 0:
            return
        elif self._refcount != 1:
            self._refcount -= 1
        else:
            self._queue.put_nowait(None)
            await self._queue.join()
            self._worker_task.cancel()
            self._worker_task = None
            self._refcount = 0

    def __call__[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        self._queue.put_nowait((partial(func, *args, **kwargs), result := self._loop.create_future()))
        return result
    
    async def __aenter__(self) -> Self:
        await self.start()
        return self
    
    async def __aexit__(self, *args) -> None:
        await self.stop()
        if args[1] is not None:
            raise args[1]


@dataclass(slots=True, init=False)
class ConnectionManager(ConsistentExecutor):
    _instances: ClassVar[dict[str, Self]] = {}
    _connection_properties: dict[str, Any]
    _connection: Connection | None

    def __new__(cls, database: str | Path, **connection_properties) -> Self:
        database = str((database if isinstance(database, Path) else Path(database)).resolve())
        if database in cls._instances:
            return cls._instances[database]
        super(ConnectionManager, obj := super(ConnectionManager, cls).__new__(cls)).__init__()
        obj._connection_properties = connection_properties | {'database': database, 'check_same_thread': False}
        obj._connection = None
        cls._instances[database] = obj
        return obj
    
    def __init__(self, *_, **__) -> None:
        pass

    @property
    def database(self) -> str:
        return self._connection_properties['database']
    
    async def execute(self, sql: str, parameters: Parameters = ()) -> 'CursorManager':
        return CursorManager(self, await self(self._connection.execute, sql, parameters))
    
    async def executemany(self, sql: str, parameters: Iterable[Parameters]) -> 'CursorManager':
        return CursorManager(self, await self(self._connection.executemany, sql, parameters))

    async def executescript(self, sql_script: str) -> 'CursorManager':
        return CursorManager(self, await self(self._connection.executescript, sql_script))
    
    async def commit(self) -> None:
        await self(self._connection.commit)

    async def rollback(self) -> None:
        await self(self._connection.rollback)
    
    async def start(self) -> None:
        await super(ConnectionManager, self).start()
        if self._connection is None:
            self._connection = connect(**self._connection_properties)

    async def stop(self) -> None:
        await super(ConnectionManager, self).stop()
        if self._refcount == 0 and self._connection is not None:
            self._connection.commit()
            self._connection.close()
            self._connection = None
    
    async def remove(self) -> None:
        await self.stop()
        self._instances.pop(self.database)

    async def set_connection_properties(self, **connection_properties) -> None:
        running = self._connection is not None
        await self.stop()
        if 'database' in connection_properties:
            await self.remove()
        self._connection_properties.update(connection_properties | {'check_same_thread': False})
        if running:
            await self.start()


@dataclass(slots=True)
class CursorManager:
    connection_manager: ConnectionManager
    _cursor: Cursor
    
    async def execute(self, sql: str, parameters: Parameters = ()) -> Self:
        return replace(self, _cursor=await self.connection_manager(self._cursor.execute, sql, parameters))
    
    async def executemany(self, sql: str, parameters: Iterable[Parameters]) -> Self:
        return replace(self, _cursor=await self.connection_manager(self._cursor.executemany, sql, parameters))

    async def executescript(self, sql_script: str) -> Self:
        return replace(self, _cursor=await self.connection_manager(self._cursor.executescript, sql_script))
    
    async def fetch(self, start: int = 0, stop: int | None = None, step: int = 1) -> list:
        result = []
        try:
            i = 0
            while True:
                if i > start:
                    if stop is not None and i >= stop:
                        break
                    elif i % step == 0:
                        result.append(await anext(self))
                    else:
                        await anext(self)
                i += 1
        finally:
            return result
        
    async def fetchone(self) -> Any | None:
        return await anext(self, None)

    async def __aiter__(self) -> AsyncGenerator[Any, Any | None]:
        while True:
            try:
                yield await anext(self)
            except StopAsyncIteration:
                break

    async def __anext__(self) -> Any:
        if (result := await self.connection_manager(next, self._cursor, None)) is None:
            raise StopAsyncIteration()
        return result
