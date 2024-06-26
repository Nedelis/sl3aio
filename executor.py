from logging import exception
from dataclasses import dataclass, field
from functools import wraps
from typing import Protocol, ClassVar, Dict, Tuple, Any, Self, Optional, final, Iterable, Mapping, Sequence
from os import PathLike
from os.path import abspath
from sqlite3 import Cursor, connect, Error as SL3Error
from asyncio import Queue, sleep
from dataparser import DefaultDataType

type Parameters = Sequence[DefaultDataType] | Mapping[str, DefaultDataType]


class _ExecutorFactory(Protocol):
    def __call__(self, database: PathLike) -> 'Executor':
        ...


class _ExecuteFunction(Protocol):
    async def __call__(self, executor: 'Executor', sql: str, parameters: Parameters, **conn_kwargs) -> Cursor | None:
        ...


class _RunIterationFunction(Protocol):
    async def __call__(self, executor: 'Executor') -> None:
        ...


@final
@dataclass(slots=True, match_args=False)
class Executor:
    instances: ClassVar[Dict[PathLike, Self]] = {}
    database: PathLike
    queue: Queue[Tuple[str, Parameters, Dict[str, Any]]]
    results: Optional[Queue[Cursor | None]] = None
    execute_func: Optional[_ExecuteFunction] = None
    run_iteration: Optional[_RunIterationFunction] = None
    type_: str = ''
    running: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        self.database = abspath(self.database)

    @property
    def busy(self) -> bool:
        return not self.queue.empty()

    def set_execute(self, execute_func: _ExecuteFunction) -> None:
        self.execute_func = execute_func

    def set_run_iteration(self, run_iteration: _RunIterationFunction) -> None:
        self.run_iteration = run_iteration

    def execute_safely(self, sql: str, parameters: Parameters = (), **conn_kwargs) -> Cursor | None:
        try:
            with connect(self.database, **conn_kwargs) as conn:
                return conn.execute(sql, parameters)
        except SL3Error:
            exception(f'[{self}] Error while executing SQL query "{sql}"!')
        return None
    
    async def execute(self, sql: str, parameters: Parameters = (), **conn_kwargs) -> Cursor | None:
        return await self.execute_func(self, sql, parameters, **conn_kwargs)

    async def run(self) -> None:
        self.running = True
        while self.running:
            if self.run_iteration is None:
                await sleep(1)
                continue
            await self.run_iteration(self)


def executor_factory(func: _ExecutorFactory) -> _ExecutorFactory:
    @wraps(func)
    def wrapper(database: PathLike) -> Executor:
        database, type_ = abspath(database), func.__name__
        if database not in Executor.instances:
            executor = func(database)
            executor.type_ = type_
            Executor.instances[database] = executor
        elif (executor := Executor.instances[database]).type_ != type_:
            executor.type_ = type_
            new_executor = func(database)
            for field in executor.__slots__:
                setattr(executor, field, getattr(new_executor, field))
        return executor
    return wrapper


@executor_factory
def single_executor(database: PathLike) -> Executor:
    executor = Executor(database, Queue(1))
    @executor.set_execute
    async def _(exc: Executor, sql: str, parameters: Parameters = (), **conn_kwargs) -> None:
        await exc.queue.put((sql, parameters, conn_kwargs))
        result = exc.execute_safely(sql, *parameters, **conn_kwargs)
        await exc.queue.get()
        return result
    return executor


@executor_factory
def parallel_executor(database: PathLike) -> Executor:
    executor = Executor(database, Queue(), Queue())
    @executor.set_execute
    async def _(exc: Executor, sql: str, parameters: Parameters = (), **conn_kwargs) -> None:
        await exc.queue.put((sql, parameters, conn_kwargs))
        return await exc.results.get()
    @executor.set_run_iteration
    async def _(exc: Executor) -> None:
        sql, parameters, conn_kwargs = await exc.queue.get()
        await exc.results.put(exc.execute_safely(sql, *parameters, **conn_kwargs))
    return executor


@executor_factory
def deferred_executor(database: PathLike) -> Executor:
    executor = Executor(database, Queue())
    @executor.set_execute
    async def _(exc: Executor, sql: str, parameters: Parameters = (), **conn_kwargs) -> None:
        return await exc.queue.put((sql, parameters, conn_kwargs))
    @executor.set_run_iteration
    async def _(exc: Executor) -> None:
        sql, parameters, conn_kwargs = await exc.queue.get()
        exc.execute_safely(sql, *parameters, **conn_kwargs)
    return executor
