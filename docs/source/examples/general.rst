General Examples
================

.. rst-class:: lead

    Quickly learn the basic tools using simple examples and get to work.

----

Using existing database
-----------------------

If you already have some SQLite3 database with existing tables and you want to use it asynchronously with sl3aio,
you can create a ``SolidTable`` instance using the ``SolidTable.from_database`` method. Here's an example:

First of all, import the necessary classes from the module.

.. code-block:: python

    from sl3aio import Connector, SolidTable

.. Hint::
    :class: dropdown

    - ``Connector`` is used to make connections.
    - ``SolidTable`` represents a table inside SQLite database.

Then connect to your database and load the table(-s).

.. code-block:: python

    async with Connector('my_database.db').connection_manager() as cm:
        table = await SolidTable.from_database('my_table', cm)

.. Hint::
    :class: dropdown

    - The ``connection_manager`` method is used to create an instance of a database query executor that
      supports an asynchronous context manager that automatically opens and closes a connection.
    - The ``SolidTable.from_database`` method is used to create a ``SolidTable`` instance from an existing
      table in the database.
    
Now you can operate on the table.

----

Accessing the table
-------------------

Now that you have obtained the table using one of the methods above, you can operate on it.

.. Note::
    Before accessing the table, you must first enter the table's connection manager:

    .. code-block:: python

        async with table:
            # Your code goes here

To insert a new records into the table use one of the ``insert`` or ``insert_many`` methods:

.. code-block:: python

    async with table:
        # Insert a single record
        await table.insert(id=1, name="Alice", email="Alice@example.com", age=20)

        # Insert multiple records at once. Note that you can optionally save the inserted records.
        async for inserted_record in table.insert_many(
            {'id': 2, 'name': 'Bob', 'email': 'Bob@example.dev', 'age': 26},
            {'id': 3, 'name': 'Charlie', 'email': 'Charlie@example.dev', 'age': 37}
        ):
            print(inserted_record.id)
            # Output:
            # 2
            # 3

To select records from the table, use ``select`` method:

.. code-block:: python

    async with table:
        async for record in table.select():
            if record.id in (1, 3):
                print(record.name)
                # Output:
                # Alice
                # Charlie

You can also select records that matching concrete conditions using predicate.

.. code-block:: python

    # Create a predicate. Note that the predicate must be asynchronous.
    async def age_predicate(record):
        return 21 <= record.age <= 44


    async with table:
        # Now you can select all records, matching the predicate.
        async for record in table.select(age_predicate):
            print(record.name)
            # Output:
            # Bob
            # Charlie
        
        # Or the first matched. Note that if no record was found, returns None.
        record = await table.select_one(age_predicate)
        print(record.name)
        # Output:
        # Bob

.. Hint::
    :class: dropdown

    The ``select_one`` and ``select`` methods takes callable, that returns the boolean value based on the
    record, that is given to it, as the optional parameter.

To update records inside the table use one of the ``update_one``, ``updated`` and ``update`` methods:

.. code-block:: python

    async def id_predicate(record):
        return record.id == 2


    async with table:
        updated_record = await table.update_one(id_predicate, email='SuperBob@new_example.su')
        print(updated_record.email)
        # Output:
        # SuperBob@new_example.su

.. Hint::
    :class: dropdown

    - The ``updated`` method updates and yields all records, that matched the predicate;
      the ``update`` method updates all records, that matched the predicate without yielding them.
    - The ``update_one``, ``updated`` and ``update`` methods takes predicate as the optional parameter, and values
      to update as the keyword arguments.

To delete records from the table use one of the ``delete_one``, ``pop`` and ``delete`` methods:

.. code-block:: python

    async def name_predicate(record):
        return record.name == 'Alice'


    async with table:
        deleted_record = await table.delete_one(name_predicate)
        print(deleted_record.id, deleted_record.name)
        # Output:
        # 1 Alice

.. Hint::
    :class: dropdown

    - The ``pop`` method removes and yields all records, that matched the predicate;
      the ``delete`` method removes all records, that matched the predicate without yielding them.
    - The ``delete_one``, ``pop`` and ``delete`` methods takes predicate as the optional parameter.

.. Finally, put all the code inside the main asynchronous function and run it using ``asyncio.run`` method.

.. .. admonition:: Full code
..     :class: dropdown

..     .. code-block:: python

..         from sl3aio import Connector, SolidTable
..         from asyncio import run


..         async def age_predicate(record):
..             return 21 <= record.age <= 44


..         async def id_predicate(record):
..             return record.id == 2

        
..         async def name_predicate(record):
..             return record.name == 'Alice'

        
..         async def main():
..             async with Connector('my_database.db').connection_manager() as cm:
..                 table = await SolidTable.from_database('my_table', cm)
            
..             async with table:
..                 async for record in table.select():
..                     if record.id in (1, 3):
..                         print(record.name)
..                         # Output:
..                         # Alice
..                         # Charlie
            
..                 async for record in table.select(age_predicate):
..                     print(record.name)
..                     # Output:
..                     # Bob
..                     # Charlie
                
..                 record = await table.select_one(age_predicate)
..                 print(record.name)
..                 # Output:
..                 # Bob
            
..             async with table:
..                 updated_record = await table.update_one(id_predicate, email='SuperBob@new_example.su')
..                 print(updated_record.email)
..                 # Output:
..                 # SuperBob@new_example.su

..             async with table:
..                 deleted_record = await table.delete_one(name_predicate)
..                 print(deleted_record.id, deleted_record.name)
..                 # Output:
..                 # 1 Alice


..         run(main())
