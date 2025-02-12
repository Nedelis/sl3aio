"""
Description
-----------
This module provides asynchronous wrappers and utilities for working with SQLite databases
in Python. It offers a set of classes that enable efficient, thread-safe, and consistent
execution of SQLite operations within an asynchronous context.

The module is designed to work seamlessly with Python's asyncio framework, providing
an intuitive API for database operations in asynchronous applications.

.. Note::
    - This module is designed for use with SQLite databases and may not be suitable for other database systems.
    - All database operations are executed in a consistent order, which may impact performance in some scenarios.
    - The module leverages Python's asyncio framework, so it should be used within an asynchronous context.


Key Components
--------------
- :class:`ConsistentExecutor`: Ensures consistent order of execution for queued tasks.
- :class:`ConnectionManager`: Handles SQLite database connections asynchronously.
- :class:`CursorManager`: Manages SQLite cursor operations asynchronously.


Other Components
----------------
- :class:`Executor`: Base class for asynchronous execution of synchronous functions.
- :class:`Connector`: Manages SQLite connection parameters.
- :obj:`Parameters`: Type alias for the allowed SQL parameters.


Usage Examples
--------------
- Basic Usage with In-Memory Database:

.. code-block:: python

    import asyncio
    from sl3aio.executor import ConnectionManager, Connector

    async def main():
        connector = Connector(":memory:")
        async with ConnectionManager(connector) as cm:
            # Create a table
            await cm.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            
            # Insert data
            await cm.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            await cm.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
            
            # Query data
            cursor = await cm.execute("SELECT * FROM users")
            users = await cursor.fetch()
            
            for user in users:
                print(f"User: {user}")

    asyncio.run(main())

- Using CursorManager for Iteration:

.. code-block:: python

    import asyncio
    from sl3aio.executor import ConnectionManager, Connector

    async def main():
        connector = Connector(":memory:")
        async with ConnectionManager(connector) as cm:
            await cm.execute("CREATE TABLE numbers (n INTEGER)")
            await cm.executemany("INSERT INTO numbers VALUES (?)", [(i,) for i in range(10)])
            
            cursor = await cm.execute("SELECT * FROM numbers")
            async for row in cursor:
                print(f"Number: {row[0]}")

    asyncio.run(main())

- Transaction Management:

.. code-block:: python

    import asyncio
    from sl3aio.executor import ConnectionManager, Connector

    async def main():
        connector = Connector(":memory:")
        async with ConnectionManager(connector) as cm:
            await cm.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance REAL)")
            
            try:
                await cm.execute("INSERT INTO accounts (balance) VALUES (?)", (100,))
                await cm.execute("UPDATE accounts SET balance = balance - ? WHERE id = ?", (50, 1))
                await cm.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (50, 2))
                await cm.commit()
                print("Transaction committed successfully")
            except Exception as e:
                await cm.rollback()
                print(f"Transaction rolled back: {e}")

    asyncio.run(main())

- Using ConsistentExecutor for Ordered Task Execution:

.. code-block:: python

    import asyncio
    from sl3aio.executor import ConsistentExecutor

    async def main():
        async with ConsistentExecutor() as executor:
            def task(n):
                print(f"Executing task {n}")
                return f"Result {n}"

            futures = [executor(task, i) for i in range(5)]
            results = await asyncio.gather(*futures)
            print("Results:", results)

    asyncio.run(main())
"""
from concurrent.futures import ThreadPoolExecutor
from dataclasses import InitVar, dataclass, field, replace
from asyncio import AbstractEventLoop, Future, Queue, Task, get_running_loop, create_task
from functools import partial
from pathlib import Path
from sqlite3 import Cursor, connect, Connection
from collections.abc import AsyncGenerator, Callable, Iterable, Mapping, Sequence
from typing import Any, ClassVar, Literal, TypeAlias, Self
from .dataparser import DefaultDataType

__all__ = ['Parameters', 'Executor', 'ConsistentExecutor', 'Connector', 'ConnectionManager', 'CursorManager']

Parameters: TypeAlias = Sequence[DefaultDataType] | Mapping[str, DefaultDataType]
"""Allowed SQL request parameters type."""


@dataclass(slots=True)
class Executor:
    """A class that provides asynchronous execution capabilities for synchronous functions.

    This class uses a ThreadPoolExecutor to run synchronous functions in a separate thread,
    allowing them to be executed asynchronously without blocking the main event loop.

    Example
    -------

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
        
        run(main())

        # Output:  
        # Slept for 2 seconds

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
        """Execute the given function asynchronously in a separate thread.

        This method allows you to run any synchronous function asynchronously,
        which is particularly useful for CPU-bound tasks or operations that would
        otherwise block the event loop.

        Parameters
        ----------
        func : `Callable` [`P`, `R`]
            The function to be executed asynchronously.
        *args : `P.args`
            Positional arguments to be passed to the function.
        **kwargs : `P.kwargs`
            Keyword arguments to be passed to the function.

        Returns
        -------
        `Future` [`R`]
            A Future object representing the eventual result of the function call.
        """
        return self._loop.run_in_executor(self._executor, partial(func, *args, **kwargs))


@dataclass(slots=True)
class ConsistentExecutor(Executor):
    """A class that provides consistent asynchronous execution capabilities for synchronous functions.

    This class extends the Executor class and ensures that tasks are executed in a consistent order
    using a single-threaded executor and a queue system. It's particularly useful when you need to
    maintain the order of execution for a series of tasks.

    Example
    -------
    
    .. code-block:: python

        import asyncio
        from sl3aio.executor import ConsistentExecutor

        async def main():
            async with ConsistentExecutor() as executor:
                # Define some example functions
                def task1():
                    print("Executing task 1")
                    return "Result 1"

                def task2():
                    print("Executing task 2")
                    return "Result 2"

                # Queue the tasks
                future1 = executor(task1)
                future2 = executor(task2)

                # Wait for the results
                result1 = await future1
                result2 = await future2

                print(f"Results: {result1}, {result2}")

        asyncio.run(main())

        # Output:
        # Executing task 1
        # Executing task 2
        # Results: Result 1, Result 2

    Notes
    -----
    - The ConsistentExecutor ensures that tasks are executed in the order they are queued.
    - It uses a single thread for execution, which can be beneficial for maintaining consistency
      but may not be suitable for CPU-bound tasks that require parallelism.
    - The class implements the async context manager protocol, allowing for easy resource management.
    """
    _executor: ThreadPoolExecutor = field(init=False, default_factory=partial(ThreadPoolExecutor, max_workers=1))
    """A single-threaded executor used for running tasks."""
    _queue: Queue[tuple[Callable[[], Any], Future]] = field(init=False, default_factory=Queue)
    """A queue to store tasks before execution."""
    _worker_task: Task | None = field(init=False, default=None)
    """The task responsible for processing queued items."""
    _refcount: int = field(init=False, default=0)
    """A reference count to manage the lifecycle of the worker."""

    @property
    def running(self) -> bool:
        """Check if the worker task is currently running."""
        return self._worker_task is not None

    async def _worker(self) -> None:
        """
        The main worker coroutine that processes tasks from the queue.

        This method runs in a loop, continuously fetching tasks from the queue and executing them.
        It handles task execution, result setting, and exception handling.
        """
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
        """Start the worker task if it's not already running.

        This method increments the reference count and creates a new worker task if one doesn't exist.

        Returns
        -------
        `bool`
            `True` if a new worker task was created, `False` otherwise.
        """
        self._refcount += 1
        if self._worker_task is None:
            self._worker_task = create_task(self._worker())
            return True
        return False

    async def stop(self) -> bool:
        """Stop the worker task if there are no more references.

        This method decrements the reference count and, if it reaches zero, waits for all queued
        tasks to complete before cancelling the worker task.

        Returns
        -------
        `bool`
            `True` if the worker task was stopped, `False` otherwise.
        """
        if self._refcount > 0:
            self._refcount -= 1
            if self._refcount == 0:
                await self._queue.join()
                self._worker_task.cancel()
                self._worker_task = None
                return True
        return False

    def __call__[**P, R](self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Future[R]:
        """Queue a function for execution and return a Future.

        This method wraps the given function and its arguments into a partial function,
        creates a new Future, and adds both to the queue for later execution.

        Parameters
        ----------
        func : `Callable` [`P`, `R`]
            The function to be executed.
        *args : `P.args`
            Positional arguments for the function.
        **kwargs : `P.kwargs`
            Keyword arguments for the function.

        Returns
        -------
        `Future` [`R`]
            A Future representing the eventual result of the function call.
        """
        self._queue.put_nowait((partial(func, *args, **kwargs), result := self._loop.create_future()))
        return result
    
    async def __aenter__(self) -> Self:
        """Async context manager entry point.

        This method is called when entering an 'async with' block. It starts the worker task.

        Returns
        -------
        `Self`
            The ConsistentExecutor instance.
        """
        await self.start()
        return self
    
    async def __aexit__(self, *args) -> None:
        """
        Async context manager exit point.

        This method is called when exiting an 'async with' block. It stops the worker task
        and raises any exceptions that occurred during execution.

        Parameters
        ----------
        *args : `Any`
            Exception details (type, value, traceback) if an exception occurred.
        """
        await self.stop()
        if args[1] is not None:
            raise args[1]


@dataclass(slots=True, frozen=True)
class Connector:
    """This class allows to save and reuse parameters of the
    `sqlite3.connect <https://docs.python.org/3/library/sqlite3.html#sqlite3.connect>`_ method. Every
    attribute of this class is similar to the original parameters.

    During initialization, it normalizes the database path and ensures that it exists.

    Example
    -------
    
    .. code-block:: python

        # Create a Connector instance
        connector = Connector(database="my_database.db", timeout=10.0, autocommit=True)

        # Establish a connection to the database
        connection = connector()
    """
    dbfile: InitVar[str | bytes | Path | Literal[':memory:']]
    """Specifies the path to the database file. Every value except the ':memory:' will be interpreted as a path.

    .. Note::
        This is `InitVar`, so after initialization use ``Connector.database`` attribute instead.
    """
    timeout: float = 5.0
    detect_types: int = 0
    isolation_level: Literal['DEFERRED', 'EXCLUSIVE', 'IMMEDIATE'] | None = 'DEFERRED'
    check_same_thread: bool = False
    factory: type | None = None
    cached_statements: int = 128
    uri: bool = False
    autocommit: bool = False
    database: Path | Literal[':memory:'] = field(init=False)

    def __post_init__(self, dbfile: str | bytes | Path) -> None:
        if dbfile == ':memory:':
            object.__setattr__(self, 'database', dbfile)
            return
        elif isinstance(dbfile, str):
            dbfile = Path(dbfile)
        elif isinstance(dbfile, (bytes, bytearray)):
            dbfile = Path(dbfile.decode())
        elif isinstance(dbfile, memoryview):
            dbfile = Path(dbfile.tobytes().decode())
        object.__setattr__(self, 'database', dbfile.resolve())
        self.database.touch()
    
    def connect(self) -> Connection:
        """This method establishes and returns a connection to the database with specified parameters.
        
        Returns
        -------
        `Connection <https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection>`_
            Connection object.
        """
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
    
    def connection_manager(self) -> 'ConnectionManager':
        """This method returns a ConnectionManager instance with pre-defined connector parameter.
        
        Returns
        -------
        :class:`ConnectionManager`
            An instance of the connection manager associated with the current connector.
        """
        return ConnectionManager(self)


@dataclass(slots=True, init=False)
class ConnectionManager(ConsistentExecutor):
    """A class that manages SQLite database connections asynchronously.

    This class extends the ConsistentExecutor class and provides a consistent, thread-safe
    way to interact with SQLite databases. It ensures that database operations are executed
    in the order they are queued, using a single connection per database.

    Example
    -------
    .. code-block:: python

        import asyncio
        from sl3aio.executor import ConnectionManager, Connector

        async def main():
            # Create a Connector for an in-memory database
            connector = Connector(":memory:")
            
            # Create a ConnectionManager
            async with ConnectionManager(connector) as cm:
                # Create a table
                await cm.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                
                # Insert some data
                await cm.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
                await cm.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
                
                # Query the data
                cursor = await cm.execute("SELECT * FROM users")
                users = await cursor.fetch()
                
                for user in users:
                    print(f"User: {user}")

        asyncio.run(main())

    Notes
    -----
    - The ConnectionManager uses a singleton pattern to ensure only one instance
      per database file.
    - All database operations are executed in a consistent order using the
      underlying ConsistentExecutor.
    - The class implements the async context manager protocol for easy resource
      management.

    See Also
    --------
    :class:`CursorManager`
    :class:`Connector`
    :class:`ConsistentExecutor`
    """
    _instances: ClassVar[dict[str, Self]] = {}
    """A class-level dictionary to store singleton instances for each database."""
    _connector: Connector
    """The Connector object used to create database connections."""
    _connection: Connection | None
    """The active SQLite connection, or None if not connected."""

    def __new__(cls, connector: Connector) -> 'ConnectionManager':
        """Create or return a singleton instance for each unique database.

        Parameters
        ----------
        connector : :class:`Connector`
            The Connector object used to create the database connection.

        Returns
        -------
        :class:`ConnectionManager`
            The ConnectionManager instance for the specified database.
        """
        if (database := str(connector.database)) in cls._instances:
            return cls._instances[database]
        super(ConnectionManager, obj := super(ConnectionManager, cls).__new__(cls)).__init__()
        obj._connector = replace(connector, dbfile=connector.database, check_same_thread=False)
        obj._connection = None
        cls._instances[database] = obj
        return obj
    
    def __init__(self, *_, **__) -> None:
        pass

    @property
    def connector(self) -> Connector:
        """Get the Connector object used to create the database connection."""
        return self._connector

    @property
    def database(self) -> str:
        """Get the path to the current database."""
        return self._connector.database
    
    async def execute(self, sql: str, parameters: Parameters = ()) -> 'CursorManager':
        """Execute a SQL query with optional parameters.

        Parameters
        ----------
        sql : `str`
            The SQL query to execute.
        parameters : :obj:`Parameters`, optional
            The parameters for the SQL query.

        Returns
        -------
        :class:`CursorManager`
            A CursorManager instance for the executed query.
        """
        return CursorManager(self, await self(self._connection.execute, sql, parameters))
    
    async def executemany(self, sql: str, parameters: Iterable[Parameters]) -> 'CursorManager':
        """Execute a SQL query multiple times with different sets of parameters.

        Parameters
        ----------
        sql : `str`
            The SQL query to execute.
        parameters : `Iterable` [:obj:`Parameters`]
            An iterable of parameter sets for the SQL query.

        Returns
        -------
        :class:`CursorManager`
            A CursorManager instance for the executed queries.
        """
        return CursorManager(self, await self(self._connection.executemany, sql, parameters))

    async def executescript(self, sql_script: str) -> 'CursorManager':
        """Execute a SQL script.

        Parameters
        ----------
        sql_script : `str`
            The SQL script to execute.

        Returns
        -------
        :class:`CursorManager`
            A CursorManager instance for the executed script.
        """
        return CursorManager(self, await self(self._connection.executescript, sql_script))
    
    async def commit(self) -> None:
        """Commit the current transaction."""
        await self(self._connection.commit)

    async def rollback(self) -> None:
        """Roll back the current transaction."""
        await self(self._connection.rollback)
    
    async def start(self) -> None:
        """Start the connection manager and establish a database connection.
        
        See Also
        --------
        :meth:`ConsistentExecutor.start`
        """
        if await super(ConnectionManager, self).start():
            self._connection = self._connector.connect()

    async def stop(self) -> None:
        """Stop the connection manager and close the database connection.
        
        See Also
        --------
        :meth:`ConsistentExecutor.stop`
        """
        if await super(ConnectionManager, self).stop():
            self._connection.commit()
            self._connection.close()
            self._connection = None

    async def remove(self) -> None:
        """Remove the current instance from the singleton dictionary."""
        await self.stop()
        self._instances.pop(self.database)

    async def set_connector(self, connector: Connector) -> None:
        """Update the connector and re-establish the connection if necessary.

        Parameters
        ----------
        connector : :class:`Connector`
            The new Connector object to use.
        """
        running = self._connection is not None
        await self.stop()
        if connector.database != self._connector.database:
            await self.remove()
        self._connector = replace(connector, dbfile=connector.database, check_same_thread=False)
        if running:
            await self.start()


@dataclass(slots=True)
class CursorManager:
    """A class that manages SQLite cursor operations asynchronously.

    This class works in conjunction with the ConnectionManager to provide a consistent,
    thread-safe way to interact with SQLite cursors. It wraps cursor operations and
    ensures they are executed in the order they are called, using the underlying
    ConnectionManager's execution queue.

    Notes
    -----
    - The CursorManager is typically created by the ConnectionManager's execute methods.
    - It provides asynchronous versions of standard cursor operations like execute, 
      executemany, and fetch.
    - The class implements the async iterator protocol, allowing for easy iteration 
      over query results.

    See Also
    --------
    :class:`ConnectionManager`
    :class:`Connector`
    """
    connection_manager: ConnectionManager
    """The ConnectionManager instance associated with this cursor."""
    _cursor: Cursor
    """The underlying SQLite cursor object."""
    
    async def execute(self, sql: str, parameters: Parameters = ()) -> Self:
        """Execute a SQL query with optional parameters.

        This method executes a single SQL statement and returns a new CursorManager
        instance with the updated cursor.

        Parameters
        ----------
        sql : `str`
            The SQL query to execute.
        parameters : :obj:`Parameters`, optional
            The parameters for the SQL query.

        Returns
        -------
        :class:`CursorManager`
            A new CursorManager instance with the updated cursor.
        """
        return replace(self, _cursor=await self.connection_manager(self._cursor.execute, sql, parameters))
    
    async def executemany(self, sql: str, parameters: Iterable[Parameters]) -> Self:
        """Execute a SQL query multiple times with different sets of parameters.

        This method executes the same SQL statement for each set of parameters and
        returns a new CursorManager instance with the updated cursor.

        Parameters
        ----------
        sql : `str`
            The SQL query to execute.
        parameters : `Iterable` [:obj:`Parameters`]
            An iterable of parameter sets for the SQL query.

        Returns
        -------
        :class:`CursorManager`
            A new CursorManager instance with the updated cursor.
        """
        return replace(self, _cursor=await self.connection_manager(self._cursor.executemany, sql, parameters))

    async def executescript(self, sql_script: str) -> Self:
        """Execute a SQL script.

        This method executes multiple SQL statements in a script and returns a new
        CursorManager instance with the updated cursor.

        Parameters
        ----------
        sql_script : `str`
            The SQL script to execute.

        Returns
        -------
        :class:`CursorManager`
            A new CursorManager instance with the updated cursor.
        """
        return replace(self, _cursor=await self.connection_manager(self._cursor.executescript, sql_script))
    
    async def fetch(self, start: int = 0, stop: int | None = None, step: int = 1) -> list:
        """Fetch a range of results from the cursor.

        This method allows for pagination-like functionality by specifying start,
        stop, and step parameters.

        Parameters
        ----------
        start : `int`, optional
            The index to start fetching from (default is 0).
        stop : `int` | `None`, optional
            The index to stop fetching at (default is None, which means fetch all).
        step : `int`, optional
            The step size between fetched items (default is 1).

        Returns
        -------
        `list`
            A list of fetched results.
        """
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
        """Fetch the next row of a query result set.

        Returns
        -------
        `Any` | `None`
            The next row of a query result set, or None if no more data is available.
        """
        return await anext(self, None)

    async def __aiter__(self) -> AsyncGenerator[Any, Any | None]:
        """Implement the async iterator protocol.

        This method allows the CursorManager to be used in async for loops.

        Yields
        ------
        `Any`
            The next row of the query result set.
        """
        while True:
            try:
                yield await anext(self)
            except StopAsyncIteration:
                break

    async def __anext__(self) -> Any:
        """Implement the async iterator protocol.

        This method fetches the next row from the cursor.

        Returns
        -------
        `Any`
            The next row of the query result set.

        Raises
        ------
        `StopAsyncIteration`
            When there are no more rows to fetch.
        """
        if (result := await self.connection_manager(next, self._cursor, None)) is None:
            raise StopAsyncIteration()
        return result
