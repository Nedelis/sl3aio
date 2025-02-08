from concurrent.futures import ThreadPoolExecutor
from dataclasses import InitVar, dataclass, field, replace
from asyncio import AbstractEventLoop, Future, Queue, Task, get_running_loop, create_task
from functools import partial
from pathlib import Path
from sqlite3 import Cursor, connect, Connection
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping, Sequence
from typing import Any, ClassVar, Literal, Self
from .dataparser import DefaultDataType

__all__ = ['Parameters', 'Executor', 'ConsistentExecutor', 'Connector', 'ConnectionManager', 'CursorManager']

type Parameters = Sequence[DefaultDataType] | Mapping[str, DefaultDataType]


@dataclass(slots=True)
class Executor:
    r"""A class that provides asynchronous execution capabilities for synchronous functions.

    This class uses a ThreadPoolExecutor to run synchronous functions in a separate thread,
    allowing them to be executed asynchronously without blocking the main event loop.

    Methods
    -------
    __call__[\*\*P, R](self, func: Callable[P, R], \*args: P.args, \*\*kwargs: P.kwargs) -> Future[R]
        Executes the given function asynchronously in a separate thread.

    Examples
    --------

    .. code-block:: python
    
        import time
        from asyncio import run
        
        executor = Executor()
        
        def slow_function(duration):
            time.sleep(duration)
            return f"Slept for {duration} seconds"
        
        async def main():
            result = await executor(slow_function, 2)
            print(result)
        
        run(main())  # >>> Slept for 2 seconds

    Notes
    -----
    - The Executor class is designed to work with Python's asyncio framework.
    - It automatically uses the running event loop and creates a new ThreadPoolExecutor.
    - This class is useful for running CPU-bound or blocking I/O operations without 
      blocking the main event loop.
    """
    _loop: AbstractEventLoop = field(init=False, default_factory=get_running_loop)
    """The event loop used for scheduling coroutines and callbacks."""
    _executor: ThreadPoolExecutor = field(init=False, default_factory=ThreadPoolExecutor)
    """The thread pool used for executing synchronous functions."""

    def __call__[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        r"""Execute the given function asynchronously in a separate thread.

        This method allows you to run any synchronous function asynchronously,
        which is particularly useful for CPU-bound tasks or operations that would
        otherwise block the event loop.

        Parameters
        ----------
        func : `Callable` [`P`, `R`]
            The function to be executed asynchronously.
        \*args : `P.args`
            Positional arguments to be passed to the function.
        \*\*kwargs : `P.kwargs`
            Keyword arguments to be passed to the function.

        Returns
        -------
        `Future` [`R`]
            A Future object representing the eventual result of the function call.

        Notes
        -----
        - The function is executed in a separate thread from the thread pool.
        - The result can be awaited in an asynchronous context.
        """
        return self._loop.run_in_executor(self._executor, partial(func, *args, **kwargs))


@dataclass(slots=True)
class ConsistentExecutor(Executor):
    _executor: ThreadPoolExecutor = field(init=False, default_factory=partial(ThreadPoolExecutor, max_workers=1))
    _queue: Queue[tuple[Callable[[], Any], Future]] = field(init=False, default_factory=Queue)
    _worker_task: Task | None = field(init=False, default=None)
    _refcount: int = field(init=False, default=0)

    @property
    def running(self) -> bool:
        return self._worker_task is not None

    async def _worker(self) -> None:
        while True:
            task = await self._queue.get()
            if task[1].done():
                self._queue.task_done()
                continue
            try:
                task[1].set_result(await self._loop.run_in_executor(self._executor, task[0]))
            except Exception as e:
                task[1].set_exception(e)
            finally:
                self._queue.task_done()

    async def start(self) -> bool:
        self._refcount += 1
        if self._worker_task is None:
            self._worker_task = create_task(self._worker())
            return True
        return False

    async def stop(self) -> bool:
        if self._refcount > 0:
            self._refcount -= 1
            if self._refcount == 0:
                await self._queue.join()
                self._worker_task.cancel()
                self._worker_task = None
                return True
        return False

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


@dataclass(slots=True)
class Connector:
    database: str | bytes | Path
    timeout: float = 5.0
    detect_types: int = 0
    isolation_level: Literal['DEFERRED', 'EXCLUSIVE', 'IMMEDIATE'] | None = 'DEFERRED'
    check_same_thread: bool = False
    factory: type | None = None
    cached_statements: int = 128
    uri: bool = False
    autocommit: bool = False

    def __post_init__(self) -> None:
        if isinstance(self.database, str):
            self.database = Path(self.database)
        elif isinstance(self.database, (bytes, bytearray)):
            self.database = Path(self.database.decode())
        elif isinstance(self.database, memoryview):
            self.database = Path(self.database.tobytes().decode())
        self.database = self.database.resolve()
        self.database.touch()
    
    def __call__(self) -> Connection:
        if self.factory is None:
            return connect(
                database=self.database,
                timeout=self.timeout,
                detect_types=self.detect_types,
                isolation_level=self.isolation_level,
                check_same_thread=self.check_same_thread,
                cached_statements=self.cached_statements,
                uri=self.uri,
                autocommit=self.autocommit
            )
        return connect(
            database=self.database,
            timeout=self.timeout,
            detect_types=self.detect_types,
            isolation_level=self.isolation_level,
            check_same_thread=self.check_same_thread,
            factory=self.factory,
            cached_statements=self.cached_statements,
            uri=self.uri,
            autocommit=self.autocommit
        )


@dataclass(slots=True, init=False)
class ConnectionManager(ConsistentExecutor):
    _instances: ClassVar[dict[str, Self]] = {}
    _connector: Connector
    _connection: Connection | None

    def __new__(cls, connector: Connector) -> Self:
        if (database := str(connector.database)) in cls._instances:
            return cls._instances[database]
        super(ConnectionManager, obj := super(ConnectionManager, cls).__new__(cls)).__init__()
        obj._connector = replace(connector, check_same_thread=False)
        obj._connection = None
        cls._instances[database] = obj
        return obj
    
    def __init__(self, *_, **__) -> None:
        pass

    @property
    def connector(self) -> Connector:
        return replace(self._connector)

    @property
    def database(self) -> str:
        return self._connector.database
    
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
        if await super(ConnectionManager, self).start():
            self._connection = self._connector()

    async def stop(self) -> None:
        if await super(ConnectionManager, self).stop():
            self._connection.commit()
            self._connection.close()
            self._connection = None

    async def remove(self) -> None:
        await self.stop()
        self._instances.pop(self.database)

    async def set_connector(self, connector: Connector) -> None:
        running = self._connection is not None
        await self.stop()
        if connector.database != self._connector.database:
            await self.remove()
        self._connector = replace(connector, check_same_thread=False)
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
