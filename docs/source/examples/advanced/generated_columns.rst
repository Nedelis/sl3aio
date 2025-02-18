:description: Generating columns values using methods created in python.

Generated columns
=================

.. rst-class:: lead

    Generate column values with python methods instead of using static default values.

----

Introduction
------------
Currently, sl3aio does not support the ``GENERATED`` directive, instead you can create a function inside the
python, registrate it as generator and then use it instead of the columns default values. This generators is
:py:class:`.TableColumnValueGenerator` instances.

----

Creating generator
------------------
Every :py:class:`.TableColumnValueGenerator` instance must have a unique name and a sync/async generating
callable that takes no parameters. 

First import the :py:class:`.TableColumnValueGenerator` class:

.. code-block:: python

    from sl3aio import TableColumnValueGenerator

Now you have two different options to create a generator.

Decorating a function
~~~~~~~~~~~~~~~~~~~~~
You can decorate a generating function with :py:meth:`.TableColumnValueGenerator.from_function` classmethod
to create generator:

.. code-block:: python

    @TableColumnValueGenerator.from_function('my_generator')
    def my_generator() -> str:
        # Put your implementation here
        return 'my_value'

.. Hint::
    :class: dropdown

    - The :py:meth:`.TableColumnValueGenerator.from_function` method takes following parameters:
        1. ``name``: The name of the generator.
        2. ``register``: Whether to register the generator or not, optional, default is True.

.. Note::
    The decorated function will be replaced with the class instance. To call the function you can:

    1. Use the ``next`` method on the generator (notice, that if the function was asynchronous, you must call
       this method inside the asynchronous context):

       .. code-block:: python

           print(next(my_generator))  # >>> my_value

    2. Call the function directly as the :py:attr:`.TableColumnValueGenerator.generator` attribute:

       .. code-block:: python

           print(my_generator.generator())  # >>> my_value

Passing a function
~~~~~~~~~~~~~~~~~~
You can also pass a callable directly to the :py:class:`.TableColumnValueGenerator` constructor:

.. code-block:: python

    # Generator may be async
    async def my_generator_func() -> str:
        # Put your implementation here
        return 'my_value'


    # Do not forget to register the generator, because
    # it won't be registered automatically.
    my_generator = TableColumnValueGenerator('my_generator', my_generator_func).register()

Or use :py:meth:`.TableColumnValueGenerator.make` classmethod to register the generator automatically:

.. code-block:: python

    my_generator = TableColumnValueGenerator.make('my_generator', my_generator_func)

.. Hint::
    :class: dropdown

    The :py:meth:`.TableColumnValueGenerator.make` classmethod has optional parameter ``register`` that
    defaults to True. If it set to False, the generator won't be registered.

----

Using generator
---------------
Now when you've created and **registered** generator, you can pass it instead of the columns default values.

After that, the generator will automatically generate the value for columns on insertions, when value to
them isn't provided.

With TableColumn
~~~~~~~~~~~~~~~~
If you are creating the table columns with :py:class:`.TableColumn` class' constructor, you can pass the
generator as the ``generator`` parameter:

.. code-block:: python

    table_column = TableColumn(
        ...,
        generator=my_generator,
        ...
    )

Or, if you are instantiating columns using the :py:meth:`.TableColumn.from_sql` classmethod, you can pass
the generator in the ``default`` parameter:

.. code-block:: python

    table_column = TableColumn.from_sql(
        sql=...,
        default=my_generator
    )

With EasyColumn
~~~~~~~~~~~~~~~
If you are using the :py:class:`.EasyColumn` class, you can pass the generator as the ``default`` parameter:

.. code-block:: python

    easy_column = EasyColumn(my_generator, ...)

Or in :py:class:`.EasyTable` you can use the generator as the default value for the column without additional
constraints:

.. code-block:: python

    class MyTableMarkup(EasyTable):
        name: EasySelector[str] = my_generator

Inside SQLite
~~~~~~~~~~~~~
To link the generator to the column set its ``DEFAULT`` directive to the ``$Generated:<generator_name>`` where
``<generator_name>`` is the name of the generator:

.. code-block:: sql

    CREATE TABLE my_table (
        id INTEGER PRIMARY KEY,
        name TEXT DEFAULT "$Generated:my_generator"
    );
