:description: Creating custom table classes by inheriting.

Inheriting table classes
========================

.. rst-class:: lead

    Creating a custom table class with the custom logic.

----

Introduction
------------
If the existing implementations of the table interface are not suitable for you, you can create your own.
The base class of all tables is :py:class:`.Table` class and the base class of all sqlite tables is
:py:class:`.SqlTable`.

----

Table
-----
This class supports all of the common database operations and it determines the functionality of different
types of tables.

Let's look at the implementation of this class using an example of the the :py:class:`.MemoryTable` class.

Start by defining your class as a subclass of :py:class:`.Table` [``T``]:

.. code-block:: python

    from dataclasses import dataclass
    from sl3aio import Table, TableRecord, TableSelectionPredicate
    from abc.collections import AsyncIterator

    @dataclass(slots=True)
    class MemoryTable[T](Table[T]):
        ...

.. Tip::
    - Use the generic ``T`` to show your type checker what the data type is stored in the table.
    - The :py:class:`.MemoryTable` uses ``@dataclass(slots=True)`` to improve performance, reduce memory
      usage during the instantiation and automatically call the superclass initialization.

Define how you'll store the records. In :py:class:`.MemoryTable`, a set of records is used:

.. code-block:: python

    _records: set[TableRecord[T]]

.. Hint::
    Set is used here because the :py:class:`.TableRecord` ``__hash__`` method returns hash of the tuple of
    values of unique/primary columns, but you may choose a data structure that best fits your use case, it
    doesn't have to be a set.

Then implement all abstract methods defined in the :py:class:`.Table` class:

.. code-block:: python

    async def length(self) -> int:
        # Count the number of rows in the table.
        return len(self._records)

    async def contains(self, record: TableRecord[T]) -> bool:
        # Check whether the record exists in the table.
        return await self._executor(set.__contains__, self._records, record)

    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        # Creat a new record from **values and insert it
        # into the table.
        record = await self._record_type.make(**values)
        
        if await self.contains(record) and not ignore_existing:
            await self._executor(set.discard, self._records, record)
        
        await self._executor(set.add, self._records, record)

        return record

    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        # Select records by the given predicate.
        if predicate is None:
            for record in self._records.copy():
                yield record
        else:
            for record in self._records.copy():
                if await predicate(record):
                    yield record

    async def deleted(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        # Delete and yield the deleted records matched
        # the predicate.
        if not predicate:
            for record in self._records.copy():
                await self._executor(set.discard, self._records)
                yield record
        else:
            for record in self._records.copy():
                if predicate(record):
                    await self._executor(set.discard, self._records, record)
                    yield record

    async def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        # Update records, matched the given predicate, with
        # **to_update and yield the updated records
        if not predicate:
            for record in self._records.copy():
                new_record = await record.replace(**to_update)
                await self._executor(set.discard, self._records, record)
                await self._executor(set.add, self._records, new_record)
                yield new_record
        else:
            for record in self._records.copy():
                if await predicate(record):
                    new_record = await record.replace(**to_update)
                    await self._executor(set.discard, self._records, record)
                    await self._executor(set.add, self._records, new_record)
                    yield new_record

.. Tip::
    Notice that many operations use ``self._executor`` which is :py:class:`.ConsistentExecutor`. This is
    crucial for maintaining consistency and thread-safety async. You should use it too if you follow a similar
    logic.

.. Note::
    Implement proper handling for both cases where a predicate is provided and where it's not:

    .. code-block:: python
        
        if predicate is None:
            # Handle case without predicate
        else:
            # Handle case with predicate

You can also implement other not abstract methods of the :py:class:`.Table` and use its protected fields
(``_columns``, ``_record_type``, ``_executor``).

Also do not forget to call initialization of superclass, if you are just extending it:

.. code-block:: python
    :caption: For the regular classes

    def __init__(self):
        super().__init__()
        super().__post_init__()  # Because the table is dataclass
        # Your own logic

.. code-block:: python
    :caption: For the dataclasses

    # Only if the dataclass(..., init=False)
    def __init__(self):
        super(MemoryTable, self).__init__()
        # Your own logic

    def __post_init__(self):
        super(MemoryTable, self).__post_init__()  # Because the table is dataclass
        # Your own logic

----

SqlTable
--------
This class extends the functionality of the Table class to work with SQL databases. It provides methods for
interacting with SQL tables and manages the connection to the database.

Extending the :py:class:`.SqlTable` class is almost the same as in the previous example, except that:

- The ``_executor`` attribute is now of the type :py:class:`.ConnectionManager` and must be given when creating
  the instance of the class.
- The :py:meth:`.SqlTable.from_database` method is added, to load the table from the database by its name.
- You must provide implementations for three more methods: :py:meth:`.SqlTable.create`,
  :py:meth:`.SqlTable.drop` and :py:meth:`.SqlTable.exists`.

Let's look at the implementation of this class using an example of the the :py:class:`.SolidTable` class.

Start by defining your class as a subclass of :py:class:`.SqlTable` [``T``]:

.. code-block:: python

    from dataclasses import dataclass
    from sl3aio import SqlTable, TableRecord, TableSelectionPredicate, CursorManager
    from abc.collections import AsyncIterator

    @dataclass(slots=True)
    class SolidTable(SqlTable[T]):
        ...

.. Tip::
    - Use the generic ``T`` to show your type checker what the data type is stored in the table.
    - The :py:class:`.SolidTable` uses ``@dataclass(slots=True)`` to improve performance, reduce memory
      usage during the instantiation and automatically call the superclass initialization.

Then create helper methods and attributes:

.. code-block:: python

    _default_selector: str = field(init=False)
    
    def __post_init__(self) -> None:
        super(SolidTable, self).__post_init__()
        self._default_selector = 'WHERE ' + ' AND '.join(f'{k} = ?' for k in self._record_type.fields)

    async def _execute_where(self, query: str, record: TableRecord[T], parameters: Parameters = ()) -> CursorManager:
        if self._record_type.nonrepeating:
            key = record.nonrepeating[0]
            return await self._executor.execute(f'{query} WHERE {key} = ?', (*parameters, getattr(record, key)))
        elif None in record:
            values = await record.executor(dict, ((k, v) for k in record.fields if (v := getattr(record, k)) is not None))
            return await self._executor.execute(
                f'{query} WHERE ' + self._executor(' AND '.join, (f'{k} = ?' for k in values)),
                (*parameters, *values.values())
            )
        return await self._executor.execute(f'{query} {self._default_selector}', (*parameters, *record))

.. Hint::
    - The ``_default_selector`` attribute is used to make operations on one specific record, selecting it
      using the ``WHERE`` clause and providing values for the every column of the table.
    - The ``_execute_where`` method is used to make operations on a one specific record using the most
      efficient selecting method possible, and removing the ``None`` values from ``WHERE`` clause.
    
Now implement all abstract methods defined in the :py:class:`.SqlTable` class and abstract methods delegeted
to it from :py:class:`.Table` class:

.. code-block:: python

    async def length(self) -> int:
        # Count the number of rows in the table.
        return await (await self._executor.execute(f'SELECT MAX(rowid) FROM "{self.name}"')).fetchone()[0]

    async def contains(self, record: TableRecord[T]) -> bool:
        # Check whether the record exists in the table.
        return await (await self._execute_where(f'SELECT * FROM "{self.name}"', record)).fetchone() is not None

    async def insert(self, ignore_existing: bool = False, **values: T) -> TableRecord[T]:
        # Creat a new record from **values and insert it
        # into the table.
        record = await self._record_type.make(**values)
        await self._executor.execute(
            'INSERT OR %s INTO %s VALUES (%s)' % ('IGNORE' if ignore_existing else 'REPLACE', self.name, ', '.join('?' * len(record))),
            record
        )
        return record
    
    async def select(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        # Select records by the given predicate.
        if not predicate:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                yield await self._record_type.make(*record_data)
        else:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                if await predicate(record := await self._record_type.make(*record_data)):
                    yield record
    
    async def deleted(self, predicate: TableSelectionPredicate[T] | None = None) -> AsyncIterator[TableRecord[T]]:
        # Delete and yield the deleted records matched
        # the predicate.
        if not predicate:
            async for record_data in await self._executor.execute(f'DELETE FROM "{self.name}" RETURNING *'):
                yield await self._record_type.make(*record_data)
        else:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                if await predicate(record := await self._record_type.make(*record_data)):
                    await self._execute_where(f'DELETE FROM "{self.name}"', record)
                    yield record
    
    async def updated(self, predicate: TableSelectionPredicate[T] | None = None, **to_update: T) -> AsyncIterator[TableRecord[T]]:
        # Update records, matched the given predicate, with
        # **to_update and yield the updated records
        sql = 'UPDATE %s SET %s' % (self.name, ', '.join(f'{k} = ?' for k in to_update))
        if not predicate:
            async for record_data in await self._executor.execute(f'{sql} RETURNING *', to_update.values()):
                yield await self._record_type.make(*record_data)
        else:
            async for record_data in await self._executor.execute(f'SELECT * FROM "{self.name}"'):
                if await predicate(record := await self._record_type.make(*record_data)):
                    await self._execute_where(sql, record, to_update.values())
                    yield record

    async def exists(self) -> bool:
        # Check if the table exists in the database.
        return await bool((await self._executor.execute(
            f'SELECT COUNT(name) FROM sqlite_master WHERE type="table" AND name="{self.name}"'
        )).fetchone()[0])

    async def create(self, if_not_exists: bool = True) -> None:
        # Create a table in the database.
        await self._executor.execute(
            'CREATE TABLE ' +
            ('IF NOT EXISTS ' if if_not_exists else '') +
            f'"{self.name}" ({", ".join(column.to_sql() for column in self.columns)})'
        )
    
    async def drop(self, if_exists: bool = True) -> None:
        # Drop the table from the database.
        await self._executor.execute(
            'DROP TABLE ' +
            ('IF EXISTS ' if if_exists else '') +
            f'"{self.name}"'
        )

You should always use :py:class:`.ConnectionManager` when performing operations on the database to ensure
consistency of the operations. 

.. Note::
    Implement proper handling for both cases where a predicate is provided and where it's not:

    .. code-block:: python

        if predicate is None:
            # Handle case without predicate
        else:
            # Handle case with predicate

You can also implement other not abstract methods of the :py:class:`.SqlTable` and :py:class:`.Table` and use
theirs protected fields (``_columns``, ``_record_type``, ``_executor``).

Also do not forget to call initialization of superclass, if you are just extending it:

.. code-block:: python
    :caption: For the regular classes

    def __init__(self):
        super().__init__()
        super().__post_init__()  # Because the table is dataclass
        # Your own logic

.. code-block:: python
    :caption: For the dataclasses

    # Only if the dataclass(..., init=False)
    def __init__(self):
        super(SolidTable, self).__init__()
        # Your own logic

    def __post_init__(self):
        super(SolidTable, self).__post_init__()  # Because the table is dataclass
        # Your own logic
