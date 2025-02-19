:description: Sending async requests mannually with the sl3aio.

Sending requests mannually
==========================

.. rst-class:: lead

    Send asynchronous SQLite requests manually and adapt to any task.

----

Introduction
------------
The core of the sl3aio library is :py:class:`.ConnectionManager` and :py:class:`.CursorManager` classes which
wrap the sqlite3's ``Conenction`` and ``Cursor`` classes. You can use these classes to work with sqlite
databases asynchronously without using the high-level interfaces of the library.

----

ConnectionManager
-----------------
The :py:class:`.ConnectionManager` class is build on top of the :py:class:`.ConsistentExecutor` class,
but with an additional methods of sqlite3's ``Connection`` class and singleton pattern for every database
accessed using it.

Its use is very similar to the use of the :py:class:`.ConsistentExecutor` and sending the requests is the
same thing as sending them using ``Connection`` class. Here is an example:

.. code-block:: python

    import asyncio
    from sl3aio import Connector

    # Replace ... with the desired connection parameters.
    cm = Connector('my_database.db', ...).connection_manager()

    # Use the context manager
    async with cm:
        cursor_manager = await cm.execute('sql query', ['some', 'parameters'])
        async for row in cursor_manager:
            # Some logic with rows...
            pass

        # Do not forget about executemany
        cursor_manager = await cm.executemany(
            'sql query',
            [['some', 'parameters'], ['more', 'parameters'], ...]
        )
    
    # Or start/stop manager mannually
    cm.start()
    cursor_manager = await cm.executescript('some sql script')
    cm.stop()

.. Hint::
    The :py:class:`.Connector` class stores all of the conenction information to stop/start it automatically. 

In addition to execution methods there are some other methods from ``Connection`` class:

- :py:meth:`.ConnectionManager.commit()`: Commits the current transaction.
- :py:meth:`.ConnectionManager.rollback()`: Rolls back the current transaction.

You can also get or replace the current connector:

.. code-block:: python

    current_connector = cm.connector

    # This will stop the manager,
    # remove it from the singletons
    # if the database path was changed,
    # and will start a new manager with the new connector.
    cm.set_connector(Connector(...))

To remove the manger from the singletons list use the :py:meth:`.ConnectionManager.remove` method.

----

CursorManager
-------------
The :py:class:`.CursorManager` class is a wrapepr for the sqlite3's ``Cursor`` class but with support for the
asynchronous operations and iterations. You don't need to start/stop the cursor, because it will stop
working automatically with the manager.

First get the :py:class:`.CursorManager` instance (after executing sql query via the connection manager).

Now you can get the affected rows:

.. code-block:: python

    # Iterate over the affected rows
    async for row in cursor_manager:
        # Some logic with rows...
        pass

    # Fetch one row
    row = await cursor_manager.fetchone()
    # or
    row = await anext(cursor_manager, None)

    # Fetch all rows
    rows = await cursor_manager.fetch()

    # Sliced fetch
    # start is optional, default to 0
    # stop is optional, default to the length of the result
    # step is optional, default to 1
    rows = await cursor_manager.fetch(start=..., stop=..., step=...)

Or execute other queries with this cursor:

.. code-block:: python

    # Execute SQL query
    await cursor_manager.execute('sql query', ['some', 'parameters'])

    # Executemany SQL query
    await cursor_manager.executemany(
        'sql query',
        [['some', 'parameters'], ['more', 'parameters']]
    )

    # Execute SQL script
    await cursor_manager.executescript('some sql script')

