:description: Creating a new database using sl3aio.

Creating new database
=====================

.. rst-class:: lead

    Easily create a new database in a suitable way with the sl3aio.

----

Basically, a database is a collection of tables, so to create a new one, you need to create multiple tables
that are contained in the same database. That's why the following examples show ways to create only
one table (you can just duplicate them by analogy).

Marking up a table
------------------
Before creating a database, you need to mark up the tables in it AKA define columns for each of them.
sl3aio offers a choice of 2 ways to do this.

Via EasyColumn and EasyTable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``EasyColumn`` and ``EasyTable`` classes allow you to easily create well-typed tables and operate on them.

Import necessary classes:

.. code-block:: python

    from sl3aio import EasyTable, EasyColumn, EasySelector

.. Hint::
    :class: dropdown

    - The ``EasyTable`` class provides easy table creation and simplified access to it.
    - The ``EasyColumn`` class is used to easily define table's columns.
    - The ``EasySelector`` class is used to select records from a table in a pythonic way
      (via operator overloading) and to define the table's columns types inside
      EasyTable.

Then mark up the table and its columns by extending the EasyTable class and define the columns
as the class attributes:

.. code-block:: python

    class UsersTableMarkup(EasyTable[str | int]):
        id: EasySelector[int] = EasyColumn(default=0, primary=True, nullable=False)
        name: EasySelector[str] = EasyColumn(nullable=False)
        email: EasySelector[str] = ''
        age: EasySelector[int]
    
.. Hint::
    :class: dropdown

    - Generic, passed into the EasyTable class, defines the types, that will be contained in the table.
    - The table's columns are defined as attributes of the class according to the one of the schemes:
        1. ``<column name>: EasySelector[<column type>]``: default value is None, no constraints.
        2. ``<column name>: EasySelector[<column type>] = <default_value>``: default value is <default_value>,
           no constraints.
        3. ``<column name>: EasySelector[<column type>] = EasyColumn(...)``: default value and constraints are
           specified in the EasyColumn parameters.
    - ``EasyColumn`` parameters (can be either positional or keyword) is:
        1. ``default``: default value or default value generator of the column, optional; defaults to None.
        2. ``primary``: indicates whether the column is a primary key, optional; defaults to False.
        3. ``unique``: indicates whether the column is a unique column, optional; defaults to False.
        4. ``nullable``: indicates whether the column can have a NULL value, optional; defaults to False.

To obtain the columns of the table, call ``columns`` method on the class:

.. code-block:: python

    table_columns = UsersTableMarkup.columns()

.. Hint::
    :class: dropdown

    The method returns a tuple of TableColumn instances, that can be used to create a table.

The table markup is ready.

.. Note::
    There will also be some advantages to accessing the table if you create columns in this way.

    For example, you can get typed ``EasySelector`` with pre-pinned table for the columns just by getting
    the column as an attribute of the table:

    .. code-block:: python

        id_column_selector = UsersTableMarkup.id
        name_column_selector = UsersTableMarkup.name
        # and so on...

Creating columns manually
~~~~~~~~~~~~~~~~~~~~~~~~~
If for some reason the method described above does not suit you, you can instantiate TableColumn class directly.

Import TableColumn class:

.. code-block:: python

    from sl3aio import TableColumn

Now you can mark up a columns using either TableColumn's constructor or column's sql definition.

Option 1: Via the constructor
"""""""""""""""""""""""""""""

.. code-block:: python

    table_columns = (
        TableColumn('id', 'INT', 0, primary=True, nullable=False),
        TableColumn('name', 'TEXT', nullable=False),
        TableColumn('email', 'TEXT', ''),
        TableColumn('age', 'INT')
    )

.. Hint::   
    :class: dropdown

    The constructor takes several parameters:

    1. ``name``: the name of the column.
    2. ``typename``: the SQL type of the column.
    3. ``default``: default value of the column, optional; defaults to None.
    4. ``generator``: TableColumnValueGenerator that creates the column's default value for each
        inserted record. See `advanced examples <../advanced.html>`_ for examples.
    5. ``primary``: indicates whether the column is a primary key, optional; defaults to False.
    6. ``unique``: indicates whether the column is a unique column, optional; defaults to False.
    7. ``nullable``: indicates whether the column can have a NULL value, optional; defaults to False.

Option 2: Via the SQL definition
""""""""""""""""""""""""""""""""

.. code-block:: python

    table_columns = (
        TableColumn.from_sql('id INTEGER PRIMARY KEY NOT NULL', 0),
        TableColumn.from_sql('name TEXT NOT NULL'),
        TableColumn.from_sql('email TEXT', ''),
        TableColumn.from_sql('age INTEGER')
    )

.. Hint::
    :class: dropdown   

    The method ``from_sql`` parameters are:

    1. ``sql``: SQL definition of the column.
    2. ``default``: default value or default value generator of the column, optional; defaults to None.

The table markup is ready.

----

Creating a table
----------------
Now, when you have a table's columns, you can create a table instance using them. There are two built-in
table types in sl3aio.

.. Warning::
    Never create table instances outside of an asynchronous context (except when
    you've re-implemented their logic). This is because when creating a table, it
    needs an active asynchronous event loop.
    
    You can use lazy initialization instead:

    .. code-block:: python

        class Database:
            my_table: Table

            @classmethod
            def setup(cls) -> None:
                cls.my_table = Table('my_table', columns)
    

        async def main():
            await Database.setup()
            # Now Database.my_table is ready to use

.. Tip::
    You can specify types of data, stored in the table, inside its generic:

    .. code-block:: python
        
        table: Table[TypeA | TypeB |...] = Table('my_table', columns)

    By default, the data types will be automatically defined as a union of the columns types. For example,
    if the tuple of columns is ``tuple[TableColumn[str], TableColumn[int], TableColumn[bytes]]``,  the table
    will be defined as ``MemoryTable[str | int | bytes]``.

Memory table
~~~~~~~~~~~~
If you do not need to save the database to disk and there will not be a large number of records in it,
then creating tables in memory may be suitable for you.

Import the necessary classes:

.. code-block:: python

    from sl3aio import MemoryTable

.. Hint::
    :class: dropdown

    The ``MemoryTable`` class is used to create in-memory tables, based on python sets.

Then instantiate the ``MemoryTable`` class:

.. code-block:: python

    table = MemoryTable('my_table', columns)

.. Hint::
    :class: dropdown

    The constructor of the ``MemoryTable`` class takes the following parameters:

    1. ``name``: the name of the table.
    2. ``_columns``: a tuple of ``TableColumn`` objects that define the columns in the table.

Table is ready to work.

SQLite table
~~~~~~~~~~~~
For SQLite databases, you can use the ``Connector`` class to connect to your database and create tables using
``SolidTable`` class.

Import the necessary classes:

.. code-block:: python

    from sl3aio import Connector, SolidTable

.. Hint::
    :class: dropdown
    
    - The ``Connector`` class is used to make connections to SQLite databases.
    - The ``SolidTable`` represents a table inside SQLite database.

Then create a new connection manager for the desired database using the ``Connector.connection_manager()``
method:

.. code-block:: python

    cm = Connector('my_database.db').connection_manager()

.. Hint::
    :class: dropdown

    - The constructor of the ``Connector`` class takes the following parameters:
        1. ``dbfile``: the path to the SQLite database file.
        2. Other parameters is the same as for the sqlite3 `connect <https://docs.python.org/3/library/
           sqlite3.html#sqlite3.connect>`_ method.
    - The ``connection_manager()`` method returns a ``ConnectionManager`` object, which is used to consistentyle
      execute SQL queries and manage the database connection.

Now instantiate the ``SolidTable`` class and create a new table inside the database using the ``table.create()``
method:

.. code-block:: python

    table = SolidTable('users', columns, cm)

    async with table:
        await table.create()

.. Hint::
    :class: dropdown

    - The constructor of the ``SolidTable`` class takes the following parameters:
        1. ``name``: the name of the table.
        2. ``_columns``: a tuple of ``TableColumn`` objects that define the columns in the table.
        3. ``_executor``: a ``ConnectionManager`` object to manage the database.
    - Asynchronous context manager of the table opens and closes the connection to the database automatically.
      You can manually open/close the connection using the table's ``start_executor()`` and ``stop_executor()``
      methods.
    - The ``create()`` method creates the table in the database. You can optionally set its ``if_not_exists``
      parameter to False, then the creation will raise an exception if the table already existed in the
      database.

Table is ready to work.

SQLite :memory: table
~~~~~~~~~~~~~~~~~~~~~
If you want to create a table in SQLite :memory: (which is a temporary SQLite database stored in RAM), you can
use the ``Connector`` class to conenct to it:

.. code-block:: python

    cm = Connector(':memory:').connection_manager()
    await cm.start()

Then create the ``SolidTable`` table in it (note that we don't need to enter table's async context manager,
because the conenction must be open until we are done working with the database):

.. code-block:: python

    table = SolidTable('my_table', columns, cm)
    await table.create()

.. Important::
    Don't forget to close the database connection after you finish working with the SQLite in-memory database:

    .. code-block:: python

        await cm.stop()

    You can also remove ConnectionManager for the ``:memory:`` database using the ``remove`` method on the
    connection manager object (it will stop the manager before removal):

    .. code-block:: python

        await cm.remove()

    And keep in mind that your database will be erased after the connection is closed.

Table is ready to work.
