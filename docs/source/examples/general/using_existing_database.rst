:description: Loading an existing database with sl3aio.

Using existing database
=======================

.. rst-class:: lead

    Simply load your database into python using sl3aio and get access to
    all of the tools available in the library.

----

If you already have SQLite3 database with existing tables and you want to use it asynchronously with sl3aio,
you can load all of the tables from it using ``SolidTable`` class and its ``from_database`` method.

First import necessary classes.

.. code-block:: python

    from sl3aio import Connector, SolidTable

.. Hint::
    :class: dropdown

    - ``Connector`` is used to make connections to sqlite databases.
    - ``SolidTable`` represents a table inside SQLite database.

Now connect to the desired database and load the table(-s) from it:

.. code-block:: python

    async with Connector('my_database.db').connection_manager() as cm:
        table = await SolidTable.from_database('my_table', cm)

.. Hint::
    :class: dropdown

    - The ``connection_manager`` method is used to create an instance of a database query executor that
      supports an asynchronous context manager that automatically opens and closes a connection.
    - The ``SolidTable.from_database`` method is used to create a ``SolidTable`` instance from an existing
      table in the database.

Table is ready to work. See `how to access it <./accesing_the_table.html>`_.
