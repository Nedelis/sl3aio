:description: Accessing the tables asynchronously with sl3aio.

Accessing the table
===================

.. rst-class:: lead

    Asynchronously access the created table using convinient sl3aio interfaces.

----

When you have obtained the table, you can access it. sl3aio library provides CRUD (Create, Read, Update, Delete)
and several other operations on tables.

.. Important::
    Before accessing the table, you must first run the table's executor. You can do it with async context
    manager or with ``start_executor`` and ``stop_executor`` methods:

    .. code-block:: python

        # With async context manager
        async with table:
            # Your code goes here

        # Or with start_executor and stop_executor methods
        table.start_executor()
        # Your code goes here
        table.stop_executor()
    
.. Tip::
    Instead of manually starting and stopping the table'sexecutor, you can wrap the table in an ``EasyTable``
    class that automatically manages the executor when performing common operations.

    .. code-block:: python

        from sl3aio import EasyTable

        wrapped_table = EasyTable(table)
        await wrapped_table.<operation>

        # The same as:
        async with table:
            await table.<operation>
    
.. Tip::
    If you've created table's markup using `EasyTable and EasyColumn <./general.html#via-easycolumn-and-
    easytable>`_ classes you'd better instantiate the ``EasyTable`` via the markup class constructor:

    .. code-block:: python
        :emphasize-lines: 8

        class UsersTableMarkup(EasyTable[str | int]):
            id: EasySelector[int] = EasyColumn(default=0, primary=True, nullable=False)
            name: EasySelector[str] = EasyColumn(nullable=False)
            email: EasySelector[str] = ''
            age: EasySelector[int]

        table = Table('my_table', UsersTableMarkup.columns())
        users_table = UsersTableMarkup(table)

Inserting records
-----------------
To insert a new record in the table, you can use one of the ``insert`` or ``insert_many`` methods.

.. Hint::
    Almost every operation on the table, that modifies the records list in it, returns/yields the affected
    records. In sl3aio, records are represented by the ``TableRecord`` class. This is a subtype of a tuple
    that provides access to values not only by index, but also by column name through the ``getattr`` and
    ``getitem`` methods.

    So if you have a record ``TableRecord(id=1, name="Alice", email="Alice@example.com", age=20)``, you can
    access its values like this:

    .. code-block:: python
        
        print(record.id)  # Output: 1
        print(record['name'])  # Output: Alice
        print(record[-1])  # Output: 20
    
    You also can convert it to dictionary using ``asdict`` method and to tuple using ``astuple`` method:
    
    .. code-block:: python

        print(record.asdict())  # Output: {'id': 1, 'name': 'Alice', 'email': 'Alice@example.com', 'age': 2}
        print(record.astuple())  # Output: (1, 'Alice', 'Alice@example.com', 20)

Single at once
~~~~~~~~~~~~~~
The ``insert`` method is used to insert a single record in the table. Returns inserted record.

Parameters:

1. ``ignore_existing``: If set to true, the existing record will be updated, optional, default is True.
2. ``**values``: Values of the record's columns, given as a keyword arguments. If the value for some
   column(-s) is not specified, the column's default value will be passed instead.

Example:

.. code-block:: python

    inserted_record = await table.insert(id=1, name="Alice", email="Alice@example.com", age=20)

Multiple at once
~~~~~~~~~~~~~~~~
The ``insert_many`` method is used to insert multiple records in the table at once. Returns the asynchronous
iterator, yielding the inserted records.

.. Important::
    You must iterate other the resulted iterator, otherwise the insertion won't be performed.

Parameters:

1. ``ignore_existing``: If set to true, the existing record will be updated, optional, default is True.
2. ``*values``: Dictionaries, where each dictionary represents a record with column names as keys and
   values as values. If the value for some column(-s) is not specified, the column's default value will
   be passed instead.

Example:

.. code-block:: python

    async for inserted_record in table.insert_many(
        {'id': 2, 'name': 'Bob', 'email': 'Bob@example.dev', 'age': 26},
        {'id': 3, 'name': 'Charlie', 'email': 'Charlie@example.dev', 'age': 37}
    ):
        pass

Filtering records
-----------------
sl3aio uses predicates to determine which records should be selected/modified and which should be ignored
during operations. Predicate is an async function that takes the record as a parameter, and returns whether the
record should be selected/modified or not. There is currentrly two ways to create predicates.

Via EasySelector
~~~~~~~~~~~~~~~~
The ``EasySelector`` class allows you to create complex selection criteria in pythonic way via operator
overloading. At a start point, ``EasySelector`` has the record as the underlying object. Then you can
use operators to control the selection.

.. Note::
    The ``EasySelector`` class just like the ``EasyTable`` automatically manages the executor when performing
    common operations on the pinned table.

    You can pin the table to a selector using the ``selector.pin_table(table)`` method or pass the table
    to the constructor of the ``EasySelector`` class.

First create an instance:

.. code-block:: python

    from sl3aio import EasySelector

    selector = EasySelector[str | int]()

.. Hint::
    :class: dropdown

    - The ``EasySelector`` class constructor takes the following parameters:
        1. ``table``: The pinned table, optional, defaults to None.
        2. ``selector``: The initial selector, optional, defaults to ``lambda record: record, True``.
    - You can specify the data types of the table inside the ``EasySelector`` generic.

Now you can create a selector.

.. code-block:: python
    :caption: Getting item/attribute

    selector.<attribute name>
    selector[<item name or index or slice>]

.. code-block:: python
    :caption: Logical operations

    # These operators are responsible for the result of predicates.
    # If logical operator returns false,
    # the record will not be selected/modified.

    selector (== or != or < or > or <= or >=) <value>
    selector.(is_ or is_not_ or in_ or or_ or and_)(<value>)
    selector.not_()
    <value> in selector
    .. selector.set_ok(True or False)  # Ensure that the selector is succeeded/failed

.. code-block:: python
    :caption: Arithmetical operations

    selector (+ or - or * or / or ** or % or // or @) <value>
    (- or + or ~)selector
    (abs or round or ceil or floor or trunc or int or float or complex)(selector)

.. code-block:: python
    :caption: Binary operations

    selector (<< or >> or ^ or & or |) <value>

.. code-block:: python
    :caption: Calling the selector

    selector(*args, **kwargs)

.. code-block:: python
    :caption: Applying the other functions

    # The key_or_pos parameter specifies where the current selector's
    # object will be passed to the function.
    selector.pass_into(func, key_or_pos=..., *other_args, **other_kwargs)

.. Note::
    You can compare selectors with each other, in other words replace ``<value>`` in examples with other
    selector.

After you has finished setting up the selector, you can either perform common operations (select, update, delete)
on it, as it was a table, or you can convert it into a predicate:

.. code-block:: python

    predicate = selector.as_predicate()

To check a record against the selector, you can use the ``selector.apply(record)`` method which returns a
tuple, containing a boolean indicating if the selector matched, and the result of the selector application:

.. code-block:: python

    ok, result = selector.apply(record)

Via callable
~~~~~~~~~~~~
You can create a predicate via a callable. The callable should be asynchronous, take a record as a parameter and
return a boolean indicating if the record should be selected/modified.

.. code-block:: python

    async def my_predicate(record: TableRecord) -> bool:
        # Your implementation here

Selecting records
-----------------
To select records from the table, you can use one of these methods:

- ``select(predicate)``: Yields all of the records that matched the given predicate. **You need to iterate
  over the result for the operation to be performed.**
- ``select_one(predicate)``: Returns the first yielded by ``select`` record or None if no records was selected.

From Table or EasyTable
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python
    :caption: For Table instances

    async with table:
        async for record in table.select(predicate):
            # Your implementation here

        selected_record = await table.select_one(predicate)

.. code-block:: python
    :caption: For EasyTable instances

    async for record in table.select(predicate):
        # Your implementation here

    selected_record = await table.select_one(predicate)

.. Note::
    You don't have to always pass the predicate as an argument, it's optional. If you don't pass it, the
    ``select`` method will yield the entire table, and the ``select_one`` method will return the first record.

From EasySelector
~~~~~~~~~~~~~~~~~
Since the ``EasySelector`` is the same thing as a predicate, you don't need to pass predicate inside its
``select`` and ``select_one`` methods.

.. Note::
    If you don't have a table pinned to ``EasySelector``, you need to pass your table instead of the
    ``predicate`` argument.

.. code-block:: python

    async for record in selector.select():
        # Your implementation here

    selected_record = await selector.select_one()

Updating records
----------------
To update records in the table, you can use one of these methods:

- ``updated(predicate, **to_update)``: Updates values specified in the ``**to_update`` parameter for each record
  that matched the given predicate and yields the updated records. **You need to iterate over the result for
  operation to be performed.**
- ``update(predicate, **to_update)``: Updates values specified in the ``**to_update`` parameter for each record
  that matched the given predicate without yielding the updated records.
- ``update_one(predicate, **to_update)``: Updates values specified in the ``**to_update`` parameter for the
  first record that matched the given predicate and returns the updated record or None if no record was updated.

From Table or EasyTable
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python
    :caption: For Table instances

    async with table:
        async for record in table.updated(predicate, **to_update):
            # Your implementation here

        await table.update(predicate, **to_update)

        updated_record = await table.update_one(predicate, **to_update)

.. code-block:: python
    :caption: For EasyTable instances

    async for record in table.updated(predicate, **to_update):
        # Your implementation here

    await table.update(predicate, **to_update)

    updated_record = await table.update_one(predicate, **to_update)

.. Note::
    You don't have to always pass the predicate as an argument, it's optional. If you don't pass it, the
    ``updated`` and ``update`` methods will update every record in the table, and the ``update_one`` method
    will update the first record.

From EasySelector
~~~~~~~~~~~~~~~~~
Since the ``EasySelector`` is the same thing as a predicate, you don't need to pass predicate inside its
``updated``, ``update`` and ``update_one`` methods.

.. Note::
    If you don't have a table pinned to ``EasySelector``, you need to pass your table instead of the
    ``predicate`` argument.

.. code-block:: python

    async for record in selector.updated(**to_update):
        # Your implementation here

    await selector.update(**to_update)

    updated_record = await selector.update_one(**to_update)

Deleting records
----------------
To delete records in the table, you can use one of these methods:

- ``deleted(predicate)``: Deletes and yields removed records that matched the given predicate. **You need to
  iterate over the result for operation to be performed.**
- ``delete(predicate)``: Deletes records that matched the given predicate without yielding removed.
- ``delete_one(predicate)``: Deletes and returns the first record that matched the given predicate.

From Table or EasyTable
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python
    :caption: For Table instances

    async with table:
        async for record in table.deleted(predicate):
            # Your implementation here

        await table.delete(predicate)

        removed_record = await table.delete_one(predicate)

.. code-block:: python
    :caption: For EasyTable instances

    async for record in table.deleted(predicate):
        # Your implementation here
        
    await table.delete(predicate)
    
    removed_record = await table.delete_one(predicate)

.. Note::
    You don't have to always pass the predicate as an argument, it's optional. If you don't pass it, the
    ``deleted`` and ``delete`` methods will clear the table, and the ``delete_one`` method will delete the
    first record.

From EasySelector
~~~~~~~~~~~~~~~~~
Since the ``EasySelector`` is the same thing as a predicate, you don't need to pass predicate inside its
``deleted``, ``delete`` and ``delete_one`` methods.

.. Note::
    If you don't have a table pinned to ``EasySelector``, you need to pass your table instead of the
    ``predicate`` argument.

.. code-block:: python

    async for record in selector.deleted():
        # Your implementation here

    await selector.delete()

    removed_record = await selector.delete_one()

Other operations
----------------
There are several other operations that is currently supported by the sl3aio. Some of them are common (e.g.
must be implemented by every type of table) and the others are not.

Common
~~~~~~
This operations must be implemented by every type of table.

Length
""""""
Returns the amount of records in the table.

.. code-block:: python

    length = await table.length()

Count
""""""
Returns the amount of records in the table that matches the given predicate.

.. Note::
    If you won't specify the predicate, the result will be the same as the
    length of the table.

.. code-block:: python

    count = await table.count(predicate)

Contains
""""""""
Returns True if the table contains the given record.

.. code-block:: python

    contains = await table.contains(record)

SqlTable operations
~~~~~~~~~~~~~~~~~~~
This operations are supported only by the subclasses of the ``SqlTable`` (e.g. ``SolidTable``).

Exists
""""""
Checks if the table exists in the database.

.. code-block:: python

    exists = await table.exists()

Create
""""""
Creates the table in the database.

.. code-block:: python

    await table.create()

.. Tip::
    You can optionally set the ``if_not_exists`` parameter to False to remove ``IF NOT EXISTS`` clause from the
    creation query.

Drop
""""
Drops the table from the database.

.. code-block:: python

    await table.drop()

.. Tip::
    You can optionally set the ``if_exists`` parameter to False to remove ``IF EXISTS`` clause from the
    deletion query.
