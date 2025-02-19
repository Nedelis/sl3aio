:description: Using executors to turn sync into the async.

Using executors
===============

.. rst-class:: lead

    Release the event loop when running expensive tasks with the help of executors.

----

Introduction
------------
Python's asyncio provides an interface for "isolating" tasks that blocks current loop. The sl3aio library
uses this interface to create a slightly more convenient way to interact with it.

----

Executor
--------
The :py:class:`.Executor` class uses a ``ThreadPoolExecutor`` to run synchronous functions asynchronously.

Here is an example:

.. code-block:: python

    from time import sleep, time
    from asyncio import gather
    from sl3aio import Executor

    def expensive_task(n):
        sleep(n)
        return n

    executor = Executor()

    tasks = [executor(expensive_task, i) for i in range(5)]
    start_time = time()
    results = await gather(*tasks)
    end_time = time()

    print(f'Time taken: {end_time - start_time:.1f} seconds')
    print(results)
    # Output:
    # Time taken: 4.0 seconds
    # [0, 1, 2, 3, 4]

.. Hint::
    - The :py:meth:`.Executor.__call__` method takes the following arguments:
        1. ``function``: The function to run asynchronously.
        2. ``*args``: Positional arguments to pass to the function.
        3. ``**kwargs``: Keyword arguments to pass to the function.
    - The :py:meth:`.Executor.__call__` method returns a Future pending a result of the given function.

----

ConsistentExecutor
------------------
The :py:class:`.ConsistentExecutor` class extends the :py:class:`.Executor` class, adding consistency of
the execution. This is the best choice for running synchronous functions asynchronously without risk of them
being interrupted by themselves (I/O operations).

Here is an example:

.. code-block:: python

    from time import sleep, time
    from asyncio import gather
    from sl3aio import ConsistentExecutor

    def expensive_task(n):
        sleep(n)
        return n
    
    async with ConsistentExecutor() as ce:
        tasks = [ce(expensive_task, i) for i in range(5)]
        start_time = time()
        results = await gather(*tasks)
        end_time = time()

    print(f'Time taken: {end_time - start_time:.1f} seconds')
    print(results)
    # Output:
    # Time taken: 10.0 seconds
    # [0, 1, 2, 3, 4]

.. Note::
    Every :py:class:`.ConsistentExecutor` has its own functions queue, that means that every executor will
    ensure consistency only for the functions that was executed using the same instance of the
    :py:class:`.ConsistentExecutor`.

.. Hint::
    - The :py:meth:`.ConsistentExecutor.__call__` method is identical to the :py:meth:`.Executor.__call__` method.
    - You must either enter the executor's async context or call the :py:meth:`.ConsistentExecutor.start` method
      to start the executor's worker and :py:meth:`.ConsistentExecutor.stop` method to stop the worker.

----

Predicates
----------
You may noticed that the table selection predicate must be async even if there is no async operations
inside them. As a "compensation" in each table, the records have a unique class variable
:py:attr:`.TableRecord.executor` of type :py:class:`.Executor`. You can use it to run your synchronous
checks asynchronously:

.. code-block:: python

    def _predicate_base(record: TableRecord) -> bool:
        # Your predicate logic here
        return True

    async def predicate(record: TableRecord) -> bool:
        return await record.executor(_predicate_base, record)
