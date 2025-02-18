:description: Creating a new database using sl3aio.

Creating new database
=====================

.. rst-class:: lead

    Easily create a new database in a suitable way with the sl3aio.

----

Introduction
------------
Basically, a database is a collection of tables, so to create a new one, you need to create multiple tables
that are contained in the same database. That's why the following examples show ways to create only
one table (you can just duplicate them by analogy).

----

Marking up a table
------------------
Before creating a database, you need to mark up the tables in it AKA define columns for each of them.
sl3aio offers a choice of 2 ways to do this.

Via EasyColumn and EasyTable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
:py:class:`.EasyColumn` and :py:class:`.EasyTable` classes allow you to easily create well-typed tables and
operate on them.

Import necessary classes:

.. code-block:: python

    from sl3aio import EasyTable, EasyColumn, EasySelector

.. Hint::
    :class: dropdown

    - The :py:class:`.EasyTable` class provides easy table creation and simplified access to it.
    - The :py:class:`.EasyColumn` class is used to easily define table's columns.
    - The :py:class:`.EasySelector` class is used to select records from a table in a pythonic way
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
           that can be a static one or a `value generator <../advanced/generated_columns.html>`_,
           no constraints.
        3. ``<column name>: EasySelector[<column type>] = EasyColumn(...)``: default value and constraints are
           specified in the EasyColumn parameters.
    - The :py:class:`.EasyColumn` constructor parameters (can be either positional or keyword) is:
        1. ``default``: default value or `value generator <../advanced/generated_columns.html>`_ of the column,
           optional; defaults to None.
        2. ``primary``: indicates whether the column is a primary key, optional; defaults to False.
        3. ``unique``: indicates whether the column is a unique column, optional; defaults to False.
        4. ``nullable``: indicates whether the column can have a NULL value, optional; defaults to False.

To obtain the columns of the table, call :py:meth:`.EasyTable.columns` method of the class:

.. code-block:: python

    table_columns = UsersTableMarkup.columns()

.. Hint::
    :class: dropdown

    The method returns a tuple of :py:class:`.TableColumn` instances, that can be used to create a table.

The table markup is ready.

.. Note::
    There will also be some advantages to accessing the table if you create columns in this way.

    For example, you can get typed :py:class:`.EasySelector` with pre-pinned table for the columns just by
    getting the column as an attribute of the table:

    .. code-block:: python

        id_column_selector = UsersTableMarkup.id
        name_column_selector = UsersTableMarkup.name
        # and so on...

Creating columns manually
~~~~~~~~~~~~~~~~~~~~~~~~~
If for some reason the method described above does not suit you, you can instantiate TableColumn class directly.

Import :py:class:`TableColumn` class:

.. code-block:: python

    from sl3aio import TableColumn

Now you can mark up a columns using either :py:class:`TableColumn` constructor or column's sql definition.

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
       inserted record. See about `generated columns <../advanced/generated_columns.html>`_.
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

    The method :py:meth:`.TableColumn.from_sql` parameters are:

    1. ``sql``: SQL definition of the column.
    2. ``default``: default value or `value generator <../advanced/generated_columns.html>`_ of the column,
       optional; defaults to None.

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

    The :py:class:`.MemoryTable` class is used to create in-memory tables, based on python sets.

Then instantiate the :py:class:`.MemoryTable` class:

.. code-block:: python

    table = MemoryTable('my_table', columns)

.. Hint::
    :class: dropdown

    The constructor of the :py:class:`.MemoryTable` class takes the following parameters:

    1. ``name``: the name of the table.
    2. ``_columns``: a tuple of :py:class:`.TableColumn` objects that define the columns in the table.

Table is ready to work.

SQLite table
~~~~~~~~~~~~
For SQLite databases, you can use the :py:class:`.Connector` class to connect to your database and create
tables using :py:class:`.SolidTable` class.

Import the necessary classes:

.. code-block:: python

    from sl3aio import Connector, SolidTable

.. Hint::
    :class: dropdown
    
    - The :py:class:`.Connector` class is used to make connections to SQLite databases.
    - The :py:class:`.SolidTable` represents a table inside SQLite database.

Then create a new connection manager for the desired database using the :py:meth:`.Connector.connection_manager`
method:

.. code-block:: python

    cm = Connector('my_database.db').connection_manager()

.. Hint::
    :class: dropdown

    - The constructor of the :py:class:`.Connector` class takes the following parameters:
        1. ``dbfile``: the path to the SQLite database file.
        2. Other parameters is the same as for the sqlite3 `connect <https://docs.python.org/3/library/
           sqlite3.html#sqlite3.connect>`_ method.
    - The :py:meth:`.Connector.connection_manager` method returns a :py:class:`.ConnectionManager` object,
      which is used to consistentyle execute SQL queries and manage the database connection.

Now instantiate the :py:class:`.SolidTable` class and create a new table inside the database using the
:py:meth:`.SolidTable.create()` method:

.. code-block:: python

    table = SolidTable('users', columns, cm)

    async with table:
        await table.create()

.. Hint::
    :class: dropdown

    - The constructor of the :py:class:`.SolidTable` class takes the following parameters:
        1. ``name``: the name of the table.
        2. ``_columns``: a tuple of :py:class:`.TableColumn` objects that define the columns in the table.
        3. ``_executor``: a :py:class:`.ConnectionManager` object to manage the database.
    - Asynchronous context manager of the table opens and closes the connection to the database automatically.
      You can manually open/close the connection using the table's :py:meth:`.Table.start_executor` and
      :py:meth:`.Table.stop_executor`
      methods.
    - The :py:meth:`.SolidTable.create` method creates the table in the database.

Table is ready to work.

SQLite :memory: table
~~~~~~~~~~~~~~~~~~~~~
If you want to create a table in SQLite :memory: (which is a temporary SQLite database stored in RAM), you can
use the :py:class:`.Connector` class to conenct to it:

.. code-block:: python

    cm = Connector(':memory:').connection_manager()
    await cm.start()

Then create the :py:class:`.SolidTable` table in it (note that we don't need to enter table's async context
manager, because the conenction must be open until we are done working with the database):

.. code-block:: python

    table = SolidTable('my_table', columns, cm)
    await table.create()

.. Important::
    Don't forget to close the database connection after you finish working with the SQLite in-memory database:

    .. code-block:: python

        await cm.stop()

    You can also remove :py:class:`.ConnectionManager` for the ``:memory:`` database using the
    :py:meth:`.ConnectionManager.remove` method on the connection manager object
    (it will stop the manager before removal):

    .. code-block:: python

        await cm.remove()

    And keep in mind that your database will be erased after the connection is closed.

Table is ready to work.
